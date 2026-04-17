'use strict';
require('dotenv').config();

const express = require('express');
const cors    = require('cors');
const mysql   = require('mysql2/promise');
const amqp    = require('amqplib');

const { logger }              = require('../shared/logger');
const { requestIdMiddleware }  = require('../shared/requestId');
const { errorMiddleware, createError } = require('../shared/errorMiddleware');
const { createHttpClient }     = require('../shared/httpClient');

const PORT        = process.env.PORT            || 3004;
const ACCOUNT_SVC = process.env.ACCOUNT_SVC_URL || 'http://account-service:3002';
const MQ_URL      = process.env.MQ_URL          || 'amqp://rabbitmq';
const EXCHANGE    = 'banking_events';

const BILLERS = {
  ELECTRICITY: { name: 'Electricity Board', code: 'ELECTRICITY' },
  WATER:       { name: 'Water Authority',   code: 'WATER' },
  BROADBAND:   { name: 'Broadband ISP',     code: 'BROADBAND' },
  GAS:         { name: 'Gas Company',       code: 'GAS' }
};

let pool;
let mqChannel;
let isStarted = false;

const accountClient = createHttpClient(ACCOUNT_SVC);

async function connectWithRetry(connectFn, name, maxRetries = 10) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const r = await connectFn(); logger.info({ service: 'payment-service' }, `${name} connected`); return r;
    } catch (err) {
      const delay = Math.min(1000 * Math.pow(2, attempt - 1), 30000);
      logger.warn({ attempt, delay }, `${name} not ready, retrying...`);
      if (attempt === maxRetries) throw err;
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

async function init() {
  pool = await connectWithRetry(async () => {
    const p = mysql.createPool({
      host: process.env.DB_HOST || 'mysql', user: process.env.DB_USER || 'root',
      password: process.env.DB_PASS || 'rootpassword', database: process.env.DB_NAME || 'banking_db',
      waitForConnections: true, connectionLimit: 10, queueLimit: 0
    });
    await p.execute('SELECT 1'); return p;
  }, 'MySQL');

  await connectWithRetry(async () => {
    const conn = await amqp.connect(MQ_URL);
    mqChannel   = await conn.createChannel();
    await mqChannel.assertExchange(EXCHANGE, 'topic', { durable: true });
    return conn;
  }, 'RabbitMQ');

  startOutboxPoller();
  isStarted = true;
}

const app = express();
app.use(cors()); app.use(express.json()); app.use(requestIdMiddleware);

// ── Outbox Poller ───────────────────────────────
function startOutboxPoller() {
  setInterval(async () => {
    if (!mqChannel) return;
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [events] = await conn.execute(
        `SELECT * FROM outbox_events
           WHERE status IN ('UNPUBLISHED','FAILED') AND retry_count < 5
           ORDER BY id ASC LIMIT 10 FOR UPDATE SKIP LOCKED`
      );
      if (events.length === 0) { await conn.rollback(); conn.release(); return; }
      const ids = events.map(e => e.id);
      await conn.execute(
        `UPDATE outbox_events SET status='PROCESSING' WHERE id IN (${ids.map(() => '?').join(',')})`, ids
      );
      await conn.commit();
      conn.release();
      for (const event of events) {
        const conn2 = await pool.getConnection();
        try {
          const payloadStr = typeof event.payload === 'string' ? event.payload : JSON.stringify(event.payload);
          mqChannel.publish(EXCHANGE, event.event_type, Buffer.from(payloadStr), { persistent: true });
          await conn2.execute("UPDATE outbox_events SET status='PUBLISHED', updated_at=NOW() WHERE id=?", [event.id]);
          logger.info({ eventId: event.id, eventType: event.event_type }, 'Payment outbox event published');
        } catch (pubErr) {
          logger.error({ eventId: event.id, err: pubErr.message }, 'Failed to publish payment outbox event');
          await conn2.execute("UPDATE outbox_events SET status='FAILED', retry_count=retry_count+1, updated_at=NOW() WHERE id=?", [event.id]);
        } finally { conn2.release(); }
      }
    } catch (err) {
      logger.error({ err: err.message }, 'Payment outbox poller error');
      try { await conn.rollback(); } catch (_) {}
      conn.release();
    }
  }, 5000);
}

app.get('/health/startup',   (req, res) => res.json({ status: isStarted ? 'UP' : 'STARTING', service: 'payment-service' }));
app.get('/health/liveness',  async (req, res, next) => {
  try { await pool.execute('SELECT 1'); res.json({ status: 'UP', service: 'payment-service' }); }
  catch { next(createError(503, 'HEALTH_CHECK_FAILED', 'DB ping failed')); }
});
app.get('/health/readiness', (req, res) => res.json({ status: isStarted ? 'READY' : 'NOT_READY', service: 'payment-service' }));
app.get('/health',           (req, res) => res.json({ status: 'UP', service: 'payment-service' }));

// ── List supported billers ─────────────────────
app.get('/billers', (req, res) => {
  res.json({ success: true, billers: Object.values(BILLERS) });
});

// ── Pay Bill ───────────────────────────────────
app.post('/pay-bill', async (req, res, next) => {
  const { accountId, userId, billerCode, amount } = req.body;
  const idemKey = req.headers['idempotency-key'];
  const log = logger.child({ requestId: req.requestId, endpoint: 'pay-bill' });

  if (!accountId || !userId || !billerCode || !amount) {
    return next(createError(400, 'VALIDATION_ERROR', 'accountId, userId, billerCode, and amount are required'));
  }
  if (!BILLERS[billerCode]) {
    return next(createError(400, 'INVALID_BILLER', `Biller '${billerCode}' not supported`));
  }
  if (isNaN(amount) || Number(amount) <= 0) {
    return next(createError(400, 'VALIDATION_ERROR', 'Amount must be a positive number'));
  }

  // ── Idempotency check ──
  if (idemKey) {
    const [existing] = await pool.execute(
      'SELECT response FROM idempotency_keys WHERE idem_key=? AND user_id=? AND endpoint=?',
      [idemKey, userId, '/pay-bill']
    );
    if (existing.length > 0 && existing[0].response) {
      return res.json(JSON.parse(existing[0].response));
    }
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // Insert payment record as INITIATED
    const [payResult] = await conn.execute(
      `INSERT INTO payments (account_id, user_id, biller_code, biller_name, amount, status, idempotency_key)
         VALUES (?,?,?,?,?,'INITIATED',?)`,
      [accountId, userId, billerCode, BILLERS[billerCode].name, amount, idemKey || null]
    );
    const paymentId = payResult.insertId;

    // Write Outbox event atomically
    await conn.execute(
      "INSERT INTO outbox_events (event_type, aggregate_id, payload, status) VALUES ('PaymentInitiated',?,?,'UNPUBLISHED')",
      [String(paymentId), JSON.stringify({ paymentId, accountId, userId, billerCode, billerName: BILLERS[billerCode].name, amount })]
    );
    await conn.commit();
    conn.release();

    // Debit account
    try {
      await accountClient.post(
        `/accounts/${accountId}/debit`,
        { amount },
        { headers: { 'idempotency-key': `pay:${paymentId}:${accountId}` } }
      );
    } catch (debitErr) {
      await pool.execute("UPDATE payments SET status='FAILED', updated_at=NOW() WHERE id=?", [paymentId]);
      return next(createError(400, 'DEBIT_FAILED', debitErr.response?.data?.error?.message || 'Debit failed for bill payment'));
    }

    // Mark COMPLETED + outbox
    const conn2 = await pool.getConnection();
    try {
      await conn2.beginTransaction();
      await conn2.execute("UPDATE payments SET status='COMPLETED', updated_at=NOW() WHERE id=?", [paymentId]);
      await conn2.execute(
        "INSERT INTO outbox_events (event_type, aggregate_id, payload, status) VALUES ('PaymentCompleted',?,?,'UNPUBLISHED')",
        [String(paymentId), JSON.stringify({ paymentId, accountId, userId, billerCode, billerName: BILLERS[billerCode].name, amount })]
      );
      await conn2.commit();
    } finally { conn2.release(); }

    log.info({ paymentId, billerCode, amount }, 'Bill payment completed');
    const response = { success: true, paymentId, status: 'COMPLETED', billerName: BILLERS[billerCode].name };

    if (idemKey) {
      await pool.execute(
        'INSERT IGNORE INTO idempotency_keys (idem_key, user_id, endpoint, response) VALUES (?,?,?,?)',
        [idemKey, userId, '/pay-bill', JSON.stringify(response)]
      );
    }
    res.json(response);
  } catch (err) {
    try { await conn.rollback(); } catch (_) {}
    conn.release();
    next(err);
  }
});

// ── Get Payment History ────────────────────────
app.get('/payments/user/:userId', async (req, res, next) => {
  try {
    const [rows] = await pool.execute(
      'SELECT * FROM payments WHERE user_id=? ORDER BY created_at DESC LIMIT 50',
      [req.params.userId]
    );
    res.json({ success: true, payments: rows });
  } catch (err) { next(err); }
});

app.use(errorMiddleware);

app.listen(PORT, () => logger.info({ port: PORT }, 'payment-service listening'));

init().catch(err => { logger.fatal({ err }, 'payment-service failed'); process.exit(1); });
