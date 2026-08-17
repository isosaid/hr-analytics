/**
 * HR-Analytics — общее хранилище на Vercel.
 *
 * Работает поверх Upstash Redis (Vercel Marketplace → Upstash).
 * Переменные окружения подставляются автоматически при подключении интеграции:
 *   KV_REST_API_URL / KV_REST_API_TOKEN   (устаревшие имена, тоже поддерживаются)
 *   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
 *
 * Если переменных нет — отвечает 501, и дашборд продолжает работать
 * на локальном хранилище браузера.
 *
 * GET  /api/db?keys=users,history,logins   → { users:{ts,value}, ... }
 * POST /api/db  { key, value }             → перезапись значения
 * POST /api/db  { key, merge:[...], cap }  → слияние журнала по полю uid
 */

const PREFIX = 'hra:';
const ALLOWED = ['users', 'history', 'logins', 'edits', 'newrows', 'snapshots', 'gs', 'sheetdata'];

function creds() {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
  return url && token ? { url: url.replace(/\/$/, ''), token } : null;
}

async function redis(cmd) {
  const c = creds();
  const r = await fetch(c.url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${c.token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error(`Redis ${r.status}`);
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}

async function readKey(key) {
  const raw = await redis(['GET', PREFIX + key]);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function readMany(keys) {
  if (!keys.length) return {};
  const raw = await redis(['MGET', ...keys.map(k => PREFIX + k)]);
  const out = {};
  keys.forEach((k, i) => {
    const v = raw && raw[i];
    if (!v) { out[k] = null; return; }
    try { out[k] = JSON.parse(v); } catch { out[k] = null; }
  });
  return out;
}

async function writeKey(key, payload) {
  await redis(['SET', PREFIX + key, JSON.stringify(payload)]);
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  if (!creds()) {
    return res.status(501).json({
      ok: false,
      reason: 'no-storage',
      hint: 'Подключите Upstash Redis: Vercel → проект → Storage → Upstash. Переменные окружения добавятся сами.',
    });
  }

  try {
    if (req.method === 'GET') {
      const list = String(req.query.keys || '').split(',').map(s => s.trim()).filter(Boolean);
      const keys = list.length ? list.filter(k => ALLOWED.includes(k)) : ALLOWED;
      const out = await readMany(keys);          // один запрос MGET вместо N штук
      return res.status(200).json({ ok: true, data: out, now: Date.now() });
    }

    if (req.method === 'POST') {
      const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
      const key = body.key;
      if (!ALLOWED.includes(key)) return res.status(400).json({ ok: false, reason: 'bad-key' });

      // Слияние журналов: записи с новыми uid добавляются, существующие не дублируются.
      if (Array.isArray(body.merge)) {
        const cur = (await readKey(key)) || { ts: 0, value: [] };
        const arr = Array.isArray(cur.value) ? cur.value : [];
        const seen = new Set(arr.map(x => x && x.uid).filter(Boolean));
        let added = 0;
        for (const item of body.merge) {
          if (!item || !item.uid || seen.has(item.uid)) continue;
          seen.add(item.uid); arr.push(item); added++;
        }
        arr.sort((a, b) => String(b.ts || '').localeCompare(String(a.ts || '')));
        const cap = Math.min(Number(body.cap) || 3000, 5000);
        const value = arr.slice(0, cap);
        await writeKey(key, { ts: Date.now(), value });
        return res.status(200).json({ ok: true, added, total: value.length });
      }

      await writeKey(key, { ts: Date.now(), value: body.value });
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ ok: false, reason: 'method' });
  } catch (e) {
    return res.status(500).json({ ok: false, reason: String(e.message || e) });
  }
};
