const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";

// Binance USD-M Futures
const REST_BASE = "https://fapi.binance.com";
const WS_URL = "wss://fstream.binance.com/ws/btcusdt@depth@100ms";

// ============================================================
// ORDER BOOK STATE
// ============================================================

const bids = new Map();
const asks = new Map();

let websocket = null;

let websocketConnected = false;
let snapshotConnected = false;
let initialized = false;

let lastUpdateId = 0;
let lastEventUpdateId = 0;

let currentPrice = null;

let lastMessageAt = null;
let lastSnapshotAt = null;

let reconnectCount = 0;
let resyncCount = 0;

let pendingEvents = [];

let processing = false;


// ============================================================
// BASIC HELPERS
// ============================================================

function now() {
  return new Date().toISOString();
}


function clearBook() {
  bids.clear();
  asks.clear();
}


function setLevel(map, price, quantity) {

  const p = Number(price);
  const q = Number(quantity);

  if (!Number.isFinite(p)) return;

  if (!Number.isFinite(q)) return;

  if (q <= 0) {
    map.delete(p);
  } else {
    map.set(p, q);
  }
}


function getBestBid() {

  let best = null;

  for (const p of bids.keys()) {
    if (best === null || p > best) {
      best = p;
    }
  }

  return best;
}


function getBestAsk() {

  let best = null;

  for (const p of asks.keys()) {
    if (best === null || p < best) {
      best = p;
    }
  }

  return best;
}


function updateCurrentPrice() {

  const bestBid = getBestBid();
  const bestAsk = getBestAsk();

  if (bestBid !== null && bestAsk !== null) {
    currentPrice = (bestBid + bestAsk) / 2;
  }
}


// ============================================================
// APPLY DEPTH EVENT
// ============================================================

function applyDepthEvent(event) {

  if (!event) return;

  const U = Number(event.U);
  const u = Number(event.u);

  if (!Number.isFinite(U) || !Number.isFinite(u)) {
    return;
  }

  for (const level of event.b || []) {

    if (!Array.isArray(level) || level.length < 2) {
      continue;
    }

    setLevel(bids, level[0], level[1]);
  }


  for (const level of event.a || []) {

    if (!Array.isArray(level) || level.length < 2) {
      continue;
    }

    setLevel(asks, level[0], level[1]);
  }

  lastEventUpdateId = u;

  updateCurrentPrice();
}


// ============================================================
// GET REST SNAPSHOT
// ============================================================

async function getSnapshot() {

  snapshotConnected = false;

  const url =
    REST_BASE +
    "/fapi/v1/depth" +
    "?symbol=" +
    SYMBOL +
    "&limit=1000";

  console.log("Requesting Binance Futures snapshot...");

  const response = await fetch(url);

  if (!response.ok) {

    throw new Error(
      "Binance snapshot HTTP " + response.status
    );
  }

  const data = await response.json();

  if (!data || !data.lastUpdateId) {

    throw new Error(
      "Invalid Binance Futures snapshot"
    );
  }

  clearBook();

  const snapshotId = Number(data.lastUpdateId);

  for (const level of data.bids || []) {

    if (level.length >= 2) {
      setLevel(
        bids,
        level[0],
        level[1]
      );
    }
  }


  for (const level of data.asks || []) {

    if (level.length >= 2) {
      setLevel(
        asks,
        level[0],
        level[1]
      );
    }
  }

  lastUpdateId = snapshotId;

  lastSnapshotAt = now();

  snapshotConnected = true;

  updateCurrentPrice();

  console.log(
    "Snapshot loaded:",
    snapshotId,
    "bids:",
    bids.size,
    "asks:",
    asks.size
  );
}


// ============================================================
// INITIALIZE ORDER BOOK
// ============================================================

async function initializeBook() {

  initialized = false;

  try {

    await getSnapshot();

    // Process events that arrived while snapshot
    // was being downloaded.

    const queued = pendingEvents;

    pendingEvents = [];

    for (const event of queued) {

      const U = Number(event.U);
      const u = Number(event.u);

      // Event already included in snapshot
      if (u <= lastUpdateId) {
        continue;
      }

      // First usable event must bridge the snapshot.
      if (
        U <= lastUpdateId + 1 &&
        u >= lastUpdateId + 1
      ) {

        applyDepthEvent(event);

      } else if (U > lastUpdateId + 1) {

        console.log(
          "Snapshot/event gap detected during initialization"
        );

        await resync();

        return;
      }
    }

    initialized = true;

    console.log(
      "ORDER BOOK INITIALIZED",
      "bids:",
      bids.size,
      "asks:",
      asks.size,
      "updateId:",
      lastUpdateId
    );

  } catch (error) {

    console.log(
      "INITIALIZATION ERROR:",
      error.message
    );

    initialized = false;

    setTimeout(
      initializeBook,
      3000
    );
  }
}


// ============================================================
// RESYNC
// ============================================================

async function resync() {

  if (processing) {
    return;
  }

  processing = true;

  try {

    resyncCount++;

    console.log(
      "RESYNC #" + resyncCount
    );

    initialized = false;

    pendingEvents = [];

    clearBook();

    await getSnapshot();

    initialized = true;

    console.log(
      "RESYNC COMPLETE",
      "updateId:",
      lastUpdateId,
      "bids:",
      bids.size,
      "asks:",
      asks.size
    );

  } catch (error) {

    console.log(
      "RESYNC ERROR:",
      error.message
    );

    initialized = false;

  } finally {

    processing = false;
  }
}


// ============================================================
// HANDLE WEBSOCKET DEPTH
// ============================================================

function handleDepthEvent(event) {

  lastMessageAt = now();

  // Before initialization, save events.
  if (!initialized) {

    pendingEvents.push(event);

    // Keep memory bounded.
    if (pendingEvents.length > 10000) {
      pendingEvents.shift();
    }

    return;
  }


  const U = Number(event.U);
  const u = Number(event.u);


  // Event is completely older than our book.
  if (u <= lastUpdateId) {
    return;
  }


  // There is a missing event.
  if (U > lastUpdateId + 1) {

    console.log(
      "UPDATE GAP",
      "local:",
      lastUpdateId,
      "incoming:",
      U,
      "-",
      u
    );

    resync();

    return;
  }


  applyDepthEvent(event);
}


// ============================================================
// CONNECT WEBSOCKET
// ============================================================

function connectWebSocket() {

  console.log(
    "Connecting Binance Futures WebSocket..."
  );

  websocketConnected = false;

  try {

    websocket = new WebSocket(
      WS_URL
    );

  } catch (error) {

    console.log(
      "WebSocket creation error:",
      error.message
    );

    reconnect();

    return;
  }


  websocket.on("open", () => {

    websocketConnected = true;

    console.log(
      "🟢 Binance Futures WebSocket connected"
    );

    // Start snapshot initialization after
    // the stream is already receiving events.

    initializeBook();
  });


  websocket.on("message", data => {

    try {

      const event =
        JSON.parse(
          data.toString()
        );

      // Ignore non-depth messages.
      if (
        !event ||
        event.e !== "depthUpdate"
      ) {
        return;
      }

      handleDepthEvent(event);

    } catch (error) {

      console.log(
        "WebSocket message parse error:",
        error.message
      );
    }
  });


  websocket.on("close", () => {

    websocketConnected = false;

    initialized = false;

    console.log(
      "🔴 Binance WebSocket disconnected"
    );

    reconnect();
  });


  websocket.on("error", error => {

    websocketConnected = false;

    console.log(
      "WebSocket error:",
      error.message
    );
  });
}


// ============================================================
// RECONNECT
// ============================================================

function reconnect() {

  reconnectCount++;

  initialized = false;

  setTimeout(() => {

    connectWebSocket();

  }, 3000);
}


// ============================================================
// STATUS
// ============================================================

function getStatus() {

  const bestBid = getBestBid();
  const bestAsk = getBestAsk();

  return {

    ok: true,

    service: "BTCUSDT Futures Order Book Relay",

    version: "1.0",

    symbol: SYMBOL,

    websocketConnected,

    snapshotConnected,

    initialized,

    currentPrice,

    bestBid,

    bestAsk,

    spread:
      bestBid !== null &&
      bestAsk !== null
        ? bestAsk - bestBid
        : null,

    bidLevels: bids.size,

    askLevels: asks.size,

    lastUpdateId,

    lastEventUpdateId,

    pendingEvents:
      pendingEvents.length,

    lastMessageAt,

    lastSnapshotAt,

    reconnectCount,

    resyncCount,

    updatedAt: now()
  };
}


// ============================================================
// /status
// ============================================================

app.get("/status", (req, res) => {

  res.json(
    getStatus()
  );
});


// ============================================================
// /health
// ============================================================

app.get("/health", (req, res) => {

  res.json({

    ok: true,

    websocketConnected,

    initialized,

    currentPrice,

    updatedAt: now()

  });
});


// ============================================================
// /orderbook
// ============================================================

app.get("/orderbook", (req, res) => {

  const bestBid = getBestBid();
  const bestAsk = getBestAsk();

  res.json({

    ok: true,

    symbol: SYMBOL,

    initialized,

    currentPrice,

    bestBid,

    bestAsk,

    bids: Array.from(bids.entries())
      .sort((a, b) => b[0] - a[0])
      .slice(0, 1000)
      .map(([price, quantity]) => ({
        price,
        quantity,
        usd: price * quantity
      })),

    asks: Array.from(asks.entries())
      .sort((a, b) => a[0] - b[0])
      .slice(0, 1000)
      .map(([price, quantity]) => ({
        price,
        quantity,
        usd: price * quantity
      })),

    updatedAt: now()
  });
});


// ============================================================
// /
// ============================================================

app.get("/", (req, res) => {

  res.json({

    ok: true,

    service:
      "BTCUSDT Futures Order Book Relay",

    endpoints: [
      "/status",
      "/health",
      "/orderbook"
    ]

  });
});


// ============================================================
// START SERVER
// ============================================================

app.listen(PORT, () => {

  console.log(
    "========================================"
  );

  console.log(
    "BTCUSDT FUTURES ORDER BOOK RELAY"
  );

  console.log(
    "========================================"
  );

  console.log(
    "Port:",
    PORT
  );

  console.log(
    "Symbol:",
    SYMBOL
  );

  console.log(
    "WebSocket:",
    WS_URL
  );

  console.log(
    "========================================"
  );

  connectWebSocket();
});
