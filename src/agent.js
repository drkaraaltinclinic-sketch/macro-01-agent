/**
 * MACRO-01 — TradFi, Commodities & Risk Regime Agent
 * Source: Yahoo Finance chart API (free, no key) with defensive handling.
 * Tracks: indices, VIX, dollar, yields, gold/silver/oil/gas/copper.
 * Computes a RISK_ON / NEUTRAL / RISK_OFF regime score — the macro gate
 * SUPREME LEADER checks before sizing any position, crypto or TradFi.
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const fetch = require('node-fetch');
const http = require('http');
const cors = require('cors');
const path = require('path');

// ─── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3009;
const GECKO_URL = process.env.GECKO_URL || 'wss://gecko-01-agent-production.up.railway.app/?agent=MACRO-01';
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || '300000'); // 5 min

// The macro universe — symbol: Yahoo ticker
const INSTRUMENTS = [
  { id: 'spx',    y: '^GSPC',    name: 'S&P 500',      group: 'INDICES', fmt: 'idx' },
  { id: 'ndx',    y: '^IXIC',    name: 'Nasdaq',       group: 'INDICES', fmt: 'idx' },
  { id: 'vix',    y: '^VIX',     name: 'VIX',          group: 'INDICES', fmt: 'idx' },
  { id: 'dxy',    y: 'DX-Y.NYB', name: 'Dollar Index', group: 'FX',      fmt: 'idx' },
  { id: 'eurusd', y: 'EURUSD=X', name: 'EUR/USD',      group: 'FX',      fmt: 'fx'  },
  { id: 'usdjpy', y: 'USDJPY=X', name: 'USD/JPY',      group: 'FX',      fmt: 'fx'  },
  { id: 'usdtry', y: 'USDTRY=X', name: 'USD/TRY',      group: 'FX',      fmt: 'fx'  },
  { id: 'us10y',  y: '^TNX',     name: 'US 10Y Yield', group: 'YIELDS',  fmt: 'pct' },
  { id: 'gold',   y: 'GC=F',     name: 'Gold',         group: 'COMMODITIES', fmt: 'usd' },
  { id: 'silver', y: 'SI=F',     name: 'Silver',       group: 'COMMODITIES', fmt: 'usd' },
  { id: 'wti',    y: 'CL=F',     name: 'Oil (WTI)',    group: 'COMMODITIES', fmt: 'usd' },
  { id: 'brent',  y: 'BZ=F',     name: 'Oil (Brent)',  group: 'COMMODITIES', fmt: 'usd' },
  { id: 'natgas', y: 'NG=F',     name: 'Nat Gas',      group: 'COMMODITIES', fmt: 'usd' },
  { id: 'copper', y: 'HG=F',     name: 'Copper',       group: 'COMMODITIES', fmt: 'usd' },
];

// ─── State ───────────────────────────────────────────────────────────────────
const state = {
  startTime: Date.now(),
  geckoConnected: false,
  cycleCount: 0,
  alertCount: 0,
  quotes: {},           // id → { price, prevClose, chg1d, ... }
  regime: { score: 0, label: 'UNKNOWN', components: [], updated: null },
  alerted: {},
  lastError: null,
  errors: [],
};

// ─── Yahoo fetch (chart endpoint — keyless, needs a browser-like UA) ─────────
async function fetchQuote(inst) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(inst.y)}?interval=1d&range=5d`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36', Accept: 'application/json' },
    timeout: 15000,
  });
  if (!res.ok) throw new Error(`Yahoo ${res.status} ${inst.y}`);
  const j = await res.json();
  const r = j?.chart?.result?.[0];
  if (!r?.meta) throw new Error(`No data ${inst.y}`);
  const meta = r.meta;
  let price = meta.regularMarketPrice;
  let prev = meta.chartPreviousClose ?? meta.previousClose;
  // Fallback to the daily close series if meta is thin
  const closes = (r.indicators?.quote?.[0]?.close || []).filter(x => x != null);
  if (price == null && closes.length) price = closes[closes.length - 1];
  if (prev == null && closes.length > 1) prev = closes[closes.length - 2];
  if (price == null || prev == null || !prev) throw new Error(`Bad quote ${inst.y}`);
  // ^TNX sometimes arrives as yield×10 — normalize
  if (inst.id === 'us10y' && price > 20) { price /= 10; prev /= 10; }
  return {
    ...inst,
    price: +price,
    prevClose: +prev,
    chg1d: +(((price - prev) / prev) * 100).toFixed(2),
    chgAbs: +(price - prev).toFixed(4),
    updated: new Date().toISOString(),
  };
}

// ─── Regime Engine ────────────────────────────────────────────────────────────
function computeRegime() {
  const q = state.quotes;
  const comp = [];
  let score = 0;
  const add = (name, pts, detail) => { score += pts; comp.push({ name, pts, detail }); };

  if (q.vix) {
    const v = q.vix.price;
    add('VIX level', v < 15 ? 2 : v < 20 ? 1 : v < 25 ? -1 : -2, `${v.toFixed(1)}`);
    if (Math.abs(q.vix.chg1d) >= 8) add('VIX move', q.vix.chg1d > 0 ? -1 : 1, `${q.vix.chg1d}% 1d`);
  }
  if (q.dxy && Math.abs(q.dxy.chg1d) >= 0.3) add('Dollar', q.dxy.chg1d > 0 ? -1 : 1, `DXY ${q.dxy.chg1d}% 1d`);
  if (q.us10y) {
    const bps = q.us10y.chgAbs * 100;
    if (Math.abs(bps) >= 5) add('Yields', bps > 0 ? -1 : 1, `10Y ${bps > 0 ? '+' : ''}${bps.toFixed(0)}bps`);
  }
  if (q.spx && Math.abs(q.spx.chg1d) >= 0.5) add('Equities', q.spx.chg1d > 0 ? 1 : -1, `SPX ${q.spx.chg1d}% 1d`);
  if (q.gold && q.spx && q.gold.chg1d >= 1.5 && q.spx.chg1d < 0) add('Flight to safety', -1, `Gold +${q.gold.chg1d}%, SPX ${q.spx.chg1d}%`);

  const label = score >= 3 ? 'RISK_ON' : score <= -3 ? 'RISK_OFF' : 'NEUTRAL';
  const prev = state.regime.label;
  state.regime = { score, label, components: comp, updated: new Date().toISOString() };
  emit('REGIME', 'macro.regime', state.regime, label === 'RISK_OFF' ? 'HIGH' : 'INFO');
  if (prev !== 'UNKNOWN' && prev !== label) {
    state.alertCount++;
    emit('ALERT', 'macro.alert', {
      type: 'REGIME_SHIFT', asset: 'MACRO', message: `Risk regime shifted ${prev} → ${label} (score ${score})`,
      recommendation: label === 'RISK_OFF' ? 'Reduce sizing across all books — macro headwind' : label === 'RISK_ON' ? 'Macro tailwind — normal sizing permitted' : 'Mixed macro — neutral sizing',
    }, 'HIGH');
  }
}

// ─── Instrument alerts ────────────────────────────────────────────────────────
function checkAlerts(qt) {
  const fire = (type, msg, rec, sev = 'MED') => {
    const key = `${type}:${qt.id}`;
    const last = state.alerted[key];
    if (last && Math.sign(last) === Math.sign(qt.chg1d) && Math.abs(qt.chg1d) <= Math.abs(last) * 1.5) return;
    state.alerted[key] = qt.chg1d;
    state.alertCount++;
    emit('ALERT', 'macro.alert', { type, asset: qt.name, change1d: qt.chg1d, price: qt.price, message: msg, recommendation: rec }, sev);
  };
  if (qt.id === 'vix' && (qt.chg1d >= 10 || (qt.price >= 25 && qt.chg1d > 0))) {
    fire('VIX_SPIKE', `VIX ${qt.price.toFixed(1)} (${qt.chg1d > 0 ? '+' : ''}${qt.chg1d}% 1d) — fear rising`, 'Risk-off pressure on all risk assets incl. crypto', 'HIGH');
  }
  if (qt.id === 'dxy' && Math.abs(qt.chg1d) >= 0.6) {
    fire('DXY_BREAKOUT', `Dollar Index ${qt.chg1d > 0 ? '+' : ''}${qt.chg1d}% 1d`, qt.chg1d > 0 ? 'Strong dollar — headwind for crypto & gold' : 'Weak dollar — tailwind for crypto & gold');
  }
  if ((qt.id === 'gold' || qt.id === 'silver') && Math.abs(qt.chg1d) >= 2) {
    fire('METAL_SURGE', `${qt.name} ${qt.chg1d > 0 ? '+' : ''}${qt.chg1d}% 1d ($${qt.price.toFixed(0)})`, qt.chg1d > 0 ? 'Safe-haven bid — check XAUT/PAXG on RWA board' : 'Metals selling off');
  }
  if ((qt.id === 'wti' || qt.id === 'brent') && Math.abs(qt.chg1d) >= 3) {
    fire('OIL_SHOCK', `${qt.name} ${qt.chg1d > 0 ? '+' : ''}${qt.chg1d}% 1d ($${qt.price.toFixed(1)})`, 'Energy shock — inflation/geopolitics pulse, watch macro regime', 'HIGH');
  }
  if (qt.id === 'us10y' && Math.abs(qt.chgAbs * 100) >= 8) {
    fire('YIELD_SHOCK', `US 10Y ${qt.chgAbs > 0 ? '+' : ''}${(qt.chgAbs * 100).toFixed(0)}bps to ${qt.price.toFixed(2)}%`, qt.chgAbs > 0 ? 'Rising yields drain risk assets' : 'Falling yields support risk assets', 'HIGH');
  }
}

// ─── Cycle ────────────────────────────────────────────────────────────────────
async function cycle() {
  state.cycleCount++;
  emit('SYS', 'macro.cycle.start', { cycle: state.cycleCount, instruments: INSTRUMENTS.length });
  let ok = 0, failed = 0;
  for (const inst of INSTRUMENTS) {
    try {
      const qt = await fetchQuote(inst);
      state.quotes[inst.id] = qt;
      emit('QUOTE', 'macro.quote', qt);
      checkAlerts(qt);
      ok++;
      await new Promise(r => setTimeout(r, 350));
    } catch (err) {
      failed++;
      state.errors.push({ time: new Date().toISOString(), inst: inst.id, message: err.message });
      state.errors = state.errors.slice(-15);
    }
  }
  if (ok) computeRegime();
  emit('SYS', 'macro.cycle.complete', { cycle: state.cycleCount, ok, failed, regime: state.regime.label, score: state.regime.score });
}

// ─── App / Bus ────────────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

function broadcast(event) {
  const payload = JSON.stringify({ ...event, agentId: 'MACRO-01', timestamp: new Date().toISOString() });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(payload); });
}
function emit(type, topic, data, severity = 'INFO') {
  broadcast({ type, topic, data, severity });
  console.log(`[${new Date().toISOString()}] [${type}] [${topic}] ${JSON.stringify(data).substring(0, 120)}`);
}

// ─── GECKO link ───────────────────────────────────────────────────────────────
let geckoWs = null;
function connectGecko() {
  geckoWs = new WebSocket(GECKO_URL);
  geckoWs.on('open', () => { state.geckoConnected = true; emit('SYS', 'macro.gecko.connected', {}); });
  geckoWs.on('close', () => { state.geckoConnected = false; emit('SYS', 'macro.gecko.disconnected', {}); setTimeout(connectGecko, 5000); });
  geckoWs.on('error', () => {});
  geckoWs.on('message', () => {});
}
connectGecko();
setInterval(() => {
  if (geckoWs?.readyState === WebSocket.OPEN) {
    geckoWs.send(JSON.stringify({ type: 'PING' }));
    geckoWs.send(JSON.stringify({
      type: 'STATUS', agentId: 'MACRO-01',
      stats: { regime: state.regime.label, vix: state.quotes.vix ? state.quotes.vix.price.toFixed(1) : '—', mode: 'active' },
    }));
  }
}, 15000);

// ─── Dashboard WS ─────────────────────────────────────────────────────────────
wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type: 'SYS', topic: 'macro.handshake', agentId: 'MACRO-01', timestamp: new Date().toISOString(),
    data: {
      geckoConnected: state.geckoConnected,
      quotes: state.quotes, regime: state.regime,
      stats: { uptime: Date.now() - state.startTime, cycles: state.cycleCount, alerts: state.alertCount },
    },
  }));
  ws.on('message', raw => {
    try {
      const m = JSON.parse(raw.toString());
      if (m.type === 'PING') ws.send(JSON.stringify({ type: 'PONG', agentId: 'MACRO-01' }));
      if (m.type === 'SCAN') cycle();
    } catch (e) {}
  });
});

// ─── REST ─────────────────────────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({
  agent: 'MACRO-01', status: 'LIVE', geckoConnected: state.geckoConnected,
  uptime: Date.now() - state.startTime, cycles: state.cycleCount, alerts: state.alertCount,
  regime: state.regime, errors: state.errors.slice(-3),
}));
app.get('/macro', (_, res) => res.json({ agent: 'MACRO-01', timestamp: new Date().toISOString(), regime: state.regime, quotes: state.quotes }));
app.get('/regime', (_, res) => res.json(state.regime));
app.post('/scan', (_, res) => { cycle(); res.json({ ok: true }); });

// ─── Boot ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log('');
  console.log('╔════════════════════════════════════════════════╗');
  console.log('║     MACRO-01 TradFi & Risk Regime Agent        ║');
  console.log('║     Indices · FX · Yields · Commodities        ║');
  console.log('╚════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  HTTP   →  http://localhost:${PORT}`);
  console.log(`  Watch  →  ${INSTRUMENTS.length} instruments · cycle every ${POLL_INTERVAL_MS / 60000} min`);
  console.log('');
  cycle();
  setInterval(cycle, POLL_INTERVAL_MS);
});

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT',  () => process.exit(0));
