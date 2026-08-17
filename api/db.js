/**
 * HR-Analytics — общее хранилище (Upstash Redis через REST).
 *
 * Переменные окружения (любая из пар):
 *   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
 *   KV_REST_API_URL        / KV_REST_API_TOKEN
 *
 * GET  /api/db?diag=1                    → проверка настроек и связи с базой
 * GET  /api/db?keys=users,history        → чтение (одной командой MGET)
 * POST /api/db  {key, value}             → запись
 * POST /api/db  {key, merge:[...], cap}  → слияние журнала по uid
 */

const PREFIX = 'hra:';
const ALLOWED = ['users', 'history', 'logins', 'edits', 'newrows', 'snapshots', 'gs', 'sheetdata'];

function creds() {
  const url = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  return { url: String(url).trim().replace(/\/+$/, ''), token: String(token).trim() };
}

async function redis(cmd) {
  const c = creds();
  const r = await fetch(c.url, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + c.token, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  const text = await r.text();
  if (!r.ok) throw new Error('Upstash ' + r.status + ': ' + text.slice(0, 160));
  let j;
  try { j = JSON.parse(text); } catch { throw new Error('Upstash вернул не JSON: ' + text.slice(0, 160)); }
  if (j.error) throw new Error('Upstash: ' + j.error);
  return j.result;
}

// Разбираем запрос сами — не полагаемся на req.query / req.body.
function getQuery(req) {
  try {
    const u = new URL(req.url, 'http://x');
    const out = {};
    u.searchParams.forEach((v, k) => { out[k] = v; });
    return out;
  } catch { return {}; }
}

function getBody(req) {
  return new Promise(resolve => {
    if (req.body && typeof req.body === 'object' && !Buffer.isBuffer(req.body)) return resolve(req.body);
    if (typeof req.body === 'string') { try { return resolve(JSON.parse(req.body)); } catch { return resolve({}); } }
    let raw = '';
    req.on('data', chunk => { raw += chunk; });
    req.on('end', () => { try { resolve(JSON.parse(raw || '{}')); } catch { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  const q = getQuery(req);
  const c = creds();

  // Диагностика: открыть /api/db?diag=1 в браузере
  if (q.diag) {
    const seen = {
      UPSTASH_REDIS_REST_URL: !!process.env.UPSTASH_REDIS_REST_URL,
      UPSTASH_REDIS_REST_TOKEN: !!process.env.UPSTASH_REDIS_REST_TOKEN,
      KV_REST_API_URL: !!process.env.KV_REST_API_URL,
      KV_REST_API_TOKEN: !!process.env.KV_REST_API_TOKEN,
    };
    if (!c) return res.status(200).end(JSON.stringify({ ok: false, step: 'env', seen,
      hint: 'Переменные окружения не видны функции. Добавьте их в Production и сделайте Redeploy.' }, null, 2));
    try {
      const pong = await redis(['PING']);
      return res.status(200).end(JSON.stringify({ ok: true, step: 'ping', seen,
        host: c.url.replace(/^https?:\/\//, ''), pong, node: process.version }, null, 2));
    } catch (e) {
      return res.status(200).end(JSON.stringify({ ok: false, step: 'redis', seen,
        host: c.url.replace(/^https?:\/\//, ''), error: String(e.message || e) }, null, 2));
    }
  }

  if (!c) return res.status(501).end(JSON.stringify({ ok: false, reason: 'no-storage',
    hint: 'Добавьте UPSTASH_REDIS_REST_URL и UPSTASH_REDIS_REST_TOKEN в переменные окружения проекта.' }));

  try {
    if (req.method === 'GET') {
      const list = String(q.keys || '').split(',').map(s => s.trim()).filter(Boolean);
      const keys = list.length ? list.filter(k => ALLOWED.includes(k)) : ALLOWED;
      const out = {};
      if (keys.length) {
        const raw = await redis(['MGET', ...keys.map(k => PREFIX + k)]);
        keys.forEach((k, i) => {
          const v = raw && raw[i];
          if (!v) { out[k] = null; return; }
          try { out[k] = typeof v === 'string' ? JSON.parse(v) : v; } catch { out[k] = null; }
        });
      }
      return res.status(200).end(JSON.stringify({ ok: true, data: out, now: Date.now() }));
    }

    if (req.method === 'POST') {
      const body = await getBody(req);
      const key = body.key;
      if (!ALLOWED.includes(key)) return res.status(400).end(JSON.stringify({ ok: false, reason: 'bad-key' }));

      if (Array.isArray(body.merge)) {
        const rawCur = await redis(['GET', PREFIX + key]);
        let cur = null;
        try { cur = rawCur ? JSON.parse(rawCur) : null; } catch { cur = null; }
        const arr = (cur && Array.isArray(cur.value)) ? cur.value : [];
        const seen = new Set(arr.map(x => x && x.uid).filter(Boolean));
        let added = 0;
        for (const item of body.merge) {
          if (!item || !item.uid || seen.has(item.uid)) continue;
          seen.add(item.uid); arr.push(item); added++;
        }
        arr.sort((a, b) => String(b.ts || '').localeCompare(String(a.ts || '')));
        const cap = Math.min(Number(body.cap) || 3000, 5000);
        const value = arr.slice(0, cap);
        await redis(['SET', PREFIX + key, JSON.stringify({ ts: Date.now(), value })]);
        return res.status(200).end(JSON.stringify({ ok: true, added, total: value.length }));
      }

      await redis(['SET', PREFIX + key, JSON.stringify({ ts: Date.now(), value: body.value })]);
      return res.status(200).end(JSON.stringify({ ok: true }));
    }

    return res.status(405).end(JSON.stringify({ ok: false, reason: 'method' }));
  } catch (e) {
    return res.status(500).end(JSON.stringify({ ok: false, reason: String(e.message || e) }));
  }
};
