const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

/* ============================================================
   BINANCE CONNECTIONS
   ============================================================ */

const DEPTH_WS =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const KLINE_WS =
  `wss://fstream.binance.com/stream?streams=` +
  `${SYMBOL_LOWER}@kline_15m/` +
  `${SYMBOL_LOWER}@kline_1h/` +
  `${SYMBOL_LOWER}@kline_4h/` +
  `${SYMBOL_LOWER}@kline_1d`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* ============================================================
   GENERAL SETTINGS
   ============================================================ */

const RANGE_PCT = 0.05;

/*
   Keep the depth event buffer bounded.
*/
const MAX_PENDING_EVENTS = 2500;

/*
   Binance snapshot protection.
   We do NOT repeatedly request snapshots.
*/
const SNAPSHOT_MIN_INTERVAL_MS = 65000;
const SNAPSHOT_TIMEOUT_MS = 15000;

/* ============================================================
   RAW ORDER BOOK SETTINGS
   ============================================================ */

const RAW_BOOK_MIN_USD = 50000;

/*
   We use the order book only to confirm structural levels.
*/
const RESTING_LIQUIDITY_RADIUS_USD = 35;

const MIN_STRUCTURAL_RESTING_USD = 250000;

/* ============================================================
   MARKET STRUCTURE SETTINGS
   ============================================================ */

const STRUCTURE = {
  "15m": {
    interval: "15m",
    weight: 1,
    candles: 300,
    pivotLeft: 3,
    pivotRight: 3,
    equalTolerancePct: 0.0015
  },

  "1h": {
    interval: "1h",
    weight: 2,
    candles: 300,
    pivotLeft: 4,
    pivotRight: 4,
    equalTolerancePct: 0.0015
  },

  "4h": {
    interval: "4h",
    weight: 3,
    candles: 250,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.002
  },

  "1d": {
    interval: "1d",
    weight: 4,
    candles: 180,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.0025
  }
};

/*
   Structural levels within this distance are merged.
*/
const MERGE_LEVEL_DISTANCE_USD = 35;

/*
   Price zone around a structural level.
*/
const LEVEL_ZONE_PCT = 0.0015;

/*
   Price must come near a level to count as a touch.
*/
const TOUCH_TOLERANCE_PCT = 0.0015;

/*
   Used when measuring meaningful reaction after a swing.
*/
const REACTION_MOVE_PCT = 0.003;

/*
   Minimum score required for a major level.
*/
const MIN_MAJOR_LEVEL_SCORE = 40;

const MAX_MAJOR_LEVELS = 8;

/* ============================================================
   ORDER BOOK
   ============================================================ */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

/* ============================================================
   DEPTH WEBSOCKET
   ============================================================ */

let depthWs = null;

/* ============================================================
   SNAPSHOT WEBSOCKET API
   ============================================================ */

let snapshotWs = null;

/* ============================================================
   DEPTH SYNCHRONIZATION STATE
   ============================================================ */

let initialized = false;

let waitingForBridge = false;

let snapshotPending = false;

let gapRecovery = false;

let gapDetected = false;

let pendingDepthEvents = [];

let lastUpdateId = 0;

let snapshotRequests = 0;

let snapshot429s = 0;

let bridgeAttempts = 0;

let bridgeFound = 0;

let gapCount = 0;

let lastGapLocal = null;

let lastGapIncoming = null;

let lastSnapshotRequest = null;

let lastSnapshotResponse = null;

let lastSnapshotError = null;

let lastSnapshotId = null;

let snapshotTimer = null;

let snapshotTimeout = null;

let snapshotRequestId = 0;

/*
   Important:
   Once we enter gap recovery, do not count every
   incoming event as another gap.
*/
let gapRecoveryEventLogged = false;

/* ============================================================
   CANDLE DATA
   ============================================================ */

const candles = {
  "15m": [],
  "1h": [],
  "4h": [],
  "1d": []
};

let klineWs = null;

/*
   Historical kline requests.
*/
const historicalKlineRequests = new Map();

let historicalKlineRequestId = 50000;

/* ============================================================
   STRUCTURE STATE
   ============================================================ */

let swingHighs = [];

let swingLows = [];

let lastStructureUpdate = null;

/* ============================================================
   UTILITY FUNCTIONS
   ============================================================ */

function nowIso() {
  return new Date().toISOString();
}

function safeNumber(value) {
  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}

function round(value, decimals = 2) {
  const p = Math.pow(10, decimals);

  return Math.round(value * p) / p;
}

function pctDistance(price, reference) {
  if (
    reference === null ||
    reference === undefined ||
    reference === 0
  ) {
    return 0;
  }

  return (
    ((price - reference) / reference) *
    100
  );
}

function absolutePctDistance(price, reference) {
  return Math.abs(
    pctDistance(
      price,
      reference
    )
  );
}

function usdValue(price, quantity) {
  return price * quantity;
}

/* ============================================================
   ORDER BOOK
   ============================================================ */

function applyBookSide(map, updates) {
  if (!Array.isArray(updates)) {
    return;
  }

  for (const item of updates) {
    if (
      !Array.isArray(item) ||
      item.length < 2
    ) {
      continue;
    }

    const price =
      safeNumber(item[0]);

    const quantity =
      safeNumber(item[1]);

    if (
      price === null ||
      quantity === null
    ) {
      continue;
    }

    if (quantity === 0) {
      map.delete(price);
    } else {
      map.set(
        price,
        quantity
      );
    }
  }
}

function applyDepthEvent(event) {
  if (!event) {
    return;
  }

  applyBookSide(
    bids,
    event.b
  );

  applyBookSide(
    asks,
    event.a
  );

  if (
    event.u !== undefined
  ) {
    lastUpdateId =
      Number(event.u);
  }
}

/* ============================================================
   BEST BID / ASK
   ============================================================ */

function getBestBid() {
  let best = null;

  for (const [
    price,
    quantity
  ] of bids) {

    if (quantity <= 0) {
      continue;
    }

    if (
      best === null ||
      price > best.price
    ) {
      best = {
        price,
        quantity
      };
    }
  }

  return best;
}

function getBestAsk() {
  let best = null;

  for (const [
    price,
    quantity
  ] of asks) {

    if (quantity <= 0) {
      continue;
    }

    if (
      best === null ||
      price < best.price
    ) {
      best = {
        price,
        quantity
      };
    }
  }

  return best;
}

function getBookMid() {
  const bid =
    getBestBid();

  const ask =
    getBestAsk();

  if (
    bid &&
    ask
  ) {
    return (
      bid.price +
      ask.price
    ) / 2;
  }

  if (bid) {
    return bid.price;
  }

  if (ask) {
    return ask.price;
  }

  return currentPrice;
}

/* ============================================================
   DEPTH BUFFER
   ============================================================ */

function bufferDepthEvent(event) {
  pendingDepthEvents.push(
    event
  );

  if (
    pendingDepthEvents.length >
    MAX_PENDING_EVENTS
  ) {
    pendingDepthEvents.splice(
      0,
      pendingDepthEvents.length -
        MAX_PENDING_EVENTS
    );
  }
}

/* ============================================================
   FIND SNAPSHOT BRIDGE
   ============================================================ */

function findBridgeEvent(
  snapshotId
) {
  const target =
    Number(snapshotId) + 1;

  for (
    const event of
    pendingDepthEvents
  ) {
    const U =
      Number(event.U);

    const u =
      Number(event.u);

    if (
      !Number.isFinite(U) ||
      !Number.isFinite(u)
    ) {
      continue;
    }

    if (
      U <= target &&
      u >= target
    ) {
      return event;
    }
  }

  return null;
}

/* ============================================================
   COMPLETE SNAPSHOT BRIDGE
   ============================================================ */

function tryCompleteBridge() {
  if (
    !waitingForBridge ||
    !lastSnapshotId
  ) {
    return false;
  }

  bridgeAttempts++;

  const bridge =
    findBridgeEvent(
      lastSnapshotId
    );

  if (!bridge) {
    return false;
  }

  bridgeFound++;

  /*
     Apply the event that bridges the
     snapshot to the live stream.
  */
  applyDepthEvent(
    bridge
  );

  initialized = true;

  waitingForBridge = false;

  snapshotPending = false;

  gapRecovery = false;

  gapDetected = false;

  gapRecoveryEventLogged = false;

  /*
     Everything before the bridge is obsolete.
  */
  const bridgeIndex =
    pendingDepthEvents.indexOf(
      bridge
    );

  if (
    bridgeIndex >= 0
  ) {
    pendingDepthEvents =
      pendingDepthEvents.slice(
        bridgeIndex + 1
      );
  } else {
    pendingDepthEvents = [];
  }

  return true;
}

/* ============================================================
   SNAPSHOT RATE LIMIT
   ============================================================ */

function canRequestSnapshot() {
  if (snapshotPending) {
    return false;
  }

  if (!lastSnapshotRequest) {
    return true;
  }

  const elapsed =
    Date.now() -
    new Date(
      lastSnapshotRequest
    ).getTime();

  return (
    elapsed >=
    SNAPSHOT_MIN_INTERVAL_MS
  );
}

/* ============================================================
   REQUEST ORDER BOOK SNAPSHOT
   ============================================================ */

function requestSnapshot(
  reason = "startup"
) {
  if (
    !canRequestSnapshot()
  ) {
    return;
  }

  if (
    !snapshotWs ||
    snapshotWs.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  snapshotPending = true;

  waitingForBridge = false;

  snapshotRequests++;

  lastSnapshotRequest =
    nowIso();

  const id =
    String(
      ++snapshotRequestId
    );

  const request = {
    id,
    method: "depth",
    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  try {
    snapshotWs.send(
      JSON.stringify(
        request
      )
    );
  } catch (error) {

    snapshotPending = false;

    lastSnapshotError =
      error.message;

    return;
  }

  if (snapshotTimeout) {
    clearTimeout(
      snapshotTimeout
    );
  }

  snapshotTimeout =
    setTimeout(
      () => {

        if (
          !snapshotPending
        ) {
          return;
        }

        snapshotPending =
          false;

        lastSnapshotError =
          "Snapshot timeout";

        scheduleSnapshotRecovery();

      },
      SNAPSHOT_TIMEOUT_MS
    );
}

/* ============================================================
   INSTALL ORDER BOOK SNAPSHOT
   ============================================================ */

function installSnapshot(
  result
) {
  if (!result) {
    return false;
  }

  const snapshotId =
    Number(
      result.lastUpdateId
    );

  if (
    !Number.isFinite(
      snapshotId
    )
  ) {
    lastSnapshotError =
      "Invalid snapshot ID";

    return false;
  }

  const snapshotBids =
    Array.isArray(
      result.bids
    )
      ? result.bids
      : [];

  const snapshotAsks =
    Array.isArray(
      result.asks
    )
      ? result.asks
      : [];

  bids.clear();

  asks.clear();

  for (
    const item of
    snapshotBids
  ) {

    if (
      !Array.isArray(item)
    ) {
      continue;
    }

    const price =
      safeNumber(
        item[0]
      );

    const quantity =
      safeNumber(
        item[1]
      );

    if (
      price !== null &&
      quantity !== null &&
      quantity > 0
    ) {
      bids.set(
        price,
        quantity
      );
    }
  }

  for (
    const item of
    snapshotAsks
  ) {

    if (
      !Array.isArray(item)
    ) {
      continue;
    }

    const price =
      safeNumber(
        item[0]
      );

    const quantity =
      safeNumber(
        item[1]
      );

    if (
      price !== null &&
      quantity !== null &&
      quantity > 0
    ) {
      asks.set(
        price,
        quantity
      );
    }
  }

  lastSnapshotId =
    snapshotId;

  lastUpdateId =
    snapshotId;

  initialized = false;

  waitingForBridge = true;

  gapRecovery = false;

  lastSnapshotResponse =
    nowIso();

  lastSnapshotError =
    null;

  if (snapshotTimeout) {
    clearTimeout(
      snapshotTimeout
    );

    snapshotTimeout =
      null;
  }

  tryCompleteBridge();

  return true;
}

/* ============================================================
   DEPTH EVENT HANDLER
   ============================================================ */

function handleDepthEvent(
  event
) {
  if (
    !event ||
    event.e !==
      "depthUpdate"
  ) {
    return;
  }

  /*
     Update current price from stream.
  */
  const firstBid =
    event.b &&
    event.b.length
      ? safeNumber(
          event.b[0][0]
        )
      : null;

  const firstAsk =
    event.a &&
    event.a.length
      ? safeNumber(
          event.a[0][0]
        )
      : null;

  if (
    firstBid !== null &&
    firstAsk !== null
  ) {
    currentPrice =
      (
        firstBid +
        firstAsk
      ) / 2;
  }

  /*
     Always retain recent events.
  */
  bufferDepthEvent(
    event
  );

  /*
     Snapshot bridge not complete.
  */
  if (!initialized) {

    tryCompleteBridge();

    return;
  }

  const U =
    Number(event.U);

  const u =
    Number(event.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  /*
     Normal sequence.
  */
  if (
    U <=
      lastUpdateId + 1 &&
    u >=
      lastUpdateId + 1
  ) {

    applyDepthEvent(
      event
    );

    /*
       Successful recovery.
    */
    if (
      gapRecovery
    ) {
      gapRecovery = false;

      gapDetected = false;

      gapRecoveryEventLogged =
        false;
    }

    /*
       Remove events already applied.
    */
    while (
      pendingDepthEvents.length >
      0
    ) {

      const first =
        pendingDepthEvents[0];

      if (
        Number(first.u) <=
        lastUpdateId
      ) {
        pendingDepthEvents.shift();
      } else {
        break;
      }
    }

    return;
  }

  /*
     Event is already behind us.
  */
  if (
    u <= lastUpdateId
  ) {
    return;
  }

  /*
     IMPORTANT:
     If already recovering, don't count every
     incoming packet as another gap.
  */
  if (
    gapRecovery
  ) {

    /*
       Keep buffering until snapshot
       bridge/recovery.
    */
    return;
  }

  /*
     First genuine gap.
  */
  gapRecovery = true;

  gapDetected = true;

  gapCount++;

  gapRecoveryEventLogged =
    true;

  lastGapLocal =
    lastUpdateId;

  lastGapIncoming =
    U;

  scheduleSnapshotRecovery();
}

/* ============================================================
   SNAPSHOT RECOVERY
   ============================================================ */

function scheduleSnapshotRecovery() {

  if (snapshotTimer) {
    return;
  }

  let delay = 1000;

  if (
    !canRequestSnapshot() &&
    lastSnapshotRequest
  ) {

    const elapsed =
      Date.now() -
      new Date(
        lastSnapshotRequest
      ).getTime();

    delay =
      Math.max(
        1000,
        SNAPSHOT_MIN_INTERVAL_MS -
          elapsed
      );
  }

  snapshotTimer =
    setTimeout(
      () => {

        snapshotTimer =
          null;

        requestSnapshot(
          "gap-recovery"
        );

      },
      delay
    );
}

/* ============================================================
   DEPTH WEBSOCKET
   ============================================================ */

function connectDepthWebSocket() {

  if (depthWs) {
    try {
      depthWs.close();
    } catch (e) {}
  }

  depthWs =
    new WebSocket(
      DEPTH_WS
    );

  depthWs.on(
    "open",
    () => {

      console.log(
        "Depth WebSocket connected"
      );

      /*
         Snapshot connection is responsible
         for obtaining the initial book.
      */

      if (
        !initialized
      ) {
        scheduleSnapshotRecovery();
      }
    }
  );

  depthWs.on(
    "message",
    raw => {

      try {

        const event =
          JSON.parse(
            raw.toString()
          );

        handleDepthEvent(
          event
        );

      } catch (error) {

        console.error(
          "Depth parse error:",
          error.message
        );

      }
    }
  );

  depthWs.on(
    "close",
    () => {

      console.log(
        "Depth WebSocket closed"
      );

      setTimeout(
        () => {
          connectDepthWebSocket();
        },
        3000
      );
    }
  );

  depthWs.on(
    "error",
    error => {

      console.error(
        "Depth WebSocket error:",
        error.message
      );

    }
  );
}

/* ============================================================
   HISTORICAL KLINE REQUEST
   ============================================================ */

function requestHistoricalKlines(
  interval
) {

  if (
    !snapshotWs ||
    snapshotWs.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  const config =
    STRUCTURE[interval];

  if (!config) {
    return;
  }

  const id =
    String(
      ++historicalKlineRequestId
    );

  historicalKlineRequests.set(
    id,
    interval
  );

  const request = {
    id,
    method: "klines",
    params: {
      symbol: SYMBOL,
      interval:
        config.interval,
      limit:
        config.candles
    }
  };

  try {

    snapshotWs.send(
      JSON.stringify(
        request
      )
    );

  } catch (error) {

    historicalKlineRequests.delete(
      id
    );

    console.error(
      "Historical kline request error:",
      error.message
    );
  }
}

/* ============================================================
   HISTORICAL KLINE RESPONSE
   ============================================================ */

function handleHistoricalKlineResponse(
  message
) {

  if (
    !message ||
    message.id ===
      undefined ||
    message.id ===
      null
  ) {
    return false;
  }

  const id =
    String(
      message.id
    );

  if (
    !historicalKlineRequests.has(
      id
    )
  ) {
    return false;
  }

  const interval =
    historicalKlineRequests.get(
      id
    );

  historicalKlineRequests.delete(
    id
  );

  if (
    message.status !== 200 ||
    !Array.isArray(
      message.result
    )
  ) {

    console.error(
      `Historical ${interval} request failed`
    );

    return true;
  }

  const result =
    message.result;

  candles[interval] =
    result
      .map(
        row => ({
          openTime:
            Number(row[0]),

          open:
            Number(row[1]),

          high:
            Number(row[2]),

          low:
            Number(row[3]),

          close:
            Number(row[4]),

          volume:
            Number(row[5]),

          closeTime:
            Number(row[6]),

          closed:
            true
        })
      )
      .filter(
        candle =>
          Number.isFinite(
            candle.openTime
          ) &&
          Number.isFinite(
            candle.high
          ) &&
          Number.isFinite(
            candle.low
          ) &&
          Number.isFinite(
            candle.close
          )
      );

  console.log(
    `Loaded ${candles[interval].length} ${interval} candles`
  );

  rebuildStructure();

  return true;
}

/* ============================================================
   SNAPSHOT WEBSOCKET
   ============================================================ */

function connectSnapshotWebSocket() {

  if (snapshotWs) {
    try {
      snapshotWs.close();
    } catch (e) {}
  }

  snapshotWs =
    new WebSocket(
      WS_API
    );

  snapshotWs.on(
    "open",
    () => {

      console.log(
        "Binance Futures API WebSocket connected"
      );

      /*
         Order-book snapshot.
      */
      requestSnapshot(
        "snapshot-connected"
      );

      /*
         Historical candles.
      */
      setTimeout(
        () => {

          requestHistoricalKlines(
            "15m"
          );

          requestHistoricalKlines(
            "1h"
          );

          requestHistoricalKlines(
            "4h"
          );

          requestHistoricalKlines(
            "1d"
          );

        },
        500
      );
    }
  );

  snapshotWs.on(
    "message",
    raw => {

      try {

        const message =
          JSON.parse(
            raw.toString()
          );

        /*
           Historical candle response.
        */
        if (
          handleHistoricalKlineResponse(
            message
          )
        ) {
          return;
        }

        /*
           Rate limit.
        */
        if (
          message.status ===
            429 ||
          (
            message.error &&
            (
              message.error.code ===
                -1003 ||
              message.error.code ===
                429
            )
          )
        ) {

          snapshot429s++;

          snapshotPending =
            false;

          lastSnapshotError =
            message.error?.msg ||
            "Snapshot rate limit";

          scheduleSnapshotRecovery();

          return;
        }

        /*
           Order book snapshot.
        */
        if (
          message.status ===
            200 &&
          message.result &&
          message.result
            .lastUpdateId !==
            undefined
        ) {

          installSnapshot(
            message.result
          );

          return;
        }

        /*
           Other API error.
        */
        if (
          message.error
        ) {

          snapshotPending =
            false;

          lastSnapshotError =
            message.error.msg ||
            "Snapshot API error";

          scheduleSnapshotRecovery();
        }

      } catch (error) {

        lastSnapshotError =
          error.message;

      }
    }
  );

  snapshotWs.on(
    "close",
    () => {

      console.log(
        "Snapshot WebSocket closed"
      );

      setTimeout(
        () => {
          connectSnapshotWebSocket();
        },
        3000
      );
    }
  );

  snapshotWs.on(
    "error",
    error => {

      console.error(
        "Snapshot WebSocket error:",
        error.message
      );

    }
  );
}

/* ============================================================
   LIVE KLINE WEBSOCKET
   ============================================================ */

function connectKlineWebSocket() {

  if (klineWs) {
    try {
      klineWs.close();
    } catch (e) {}
  }

  klineWs =
    new WebSocket(
      KLINE_WS
    );

  klineWs.on(
    "open",
    () => {

      console.log(
        "Kline WebSocket connected"
      );

    }
  );

  klineWs.on(
    "message",
    raw => {

      try {

        const wrapper =
          JSON.parse(
            raw.toString()
          );

        const data =
          wrapper.data;

        if (
          !data ||
          data.e !==
            "kline"
        ) {
          return;
        }

        const k =
          data.k;

        if (!k) {
          return;
        }

        const interval =
          k.i;

        if (
          !candles[interval]
        ) {
          return;
        }

        const candle = {
          openTime:
            Number(k.t),

          closeTime:
            Number(k.T),

          open:
            Number(k.o),

          high:
            Number(k.h),

          low:
            Number(k.l),

          close:
            Number(k.c),

          volume:
            Number(k.v),

          closed:
            Boolean(k.x)
        };

        upsertCandle(
          interval,
          candle
        );

      } catch (error) {

        console.error(
          "Kline parse error:",
          error.message
        );

      }
    }
  );

  klineWs.on(
    "close",
    () => {

      console.log(
        "Kline WebSocket closed"
      );

      setTimeout(
        () => {
          connectKlineWebSocket();
        },
        3000
      );
    }
  );

  klineWs.on(
    "error",
    error => {

      console.error(
        "Kline WebSocket error:",
        error.message
      );

    }
  );
}

/* ============================================================
   UPSERT LIVE CANDLE
   ============================================================ */

function upsertCandle(
  interval,
  candle
) {

  if (
    !candles[interval]
  ) {
    candles[interval] = [];
  }

  const list =
    candles[interval];

  const index =
    list.findIndex(
      x =>
        x.openTime ===
        candle.openTime
    );

  if (
    index >= 0
  ) {

    list[index] =
      candle;

  } else {

    list.push(
      candle
    );
  }

  list.sort(
    (a, b) =>
      a.openTime -
      b.openTime
  );

  const max =
    STRUCTURE[interval]
      ?.candles || 300;

  if (
    list.length >
    max
  ) {

    list.splice(
      0,
      list.length - max
    );
  }

  /*
     Rebuild structure on closed candles.
  */
  if (
    candle.closed
  ) {
    rebuildStructure();
  }
}

/* ============================================================
   ATR
   ============================================================ */

function trueRange(
  candle,
  previous
) {

  if (!previous) {
    return (
      candle.high -
      candle.low
    );
  }

  return Math.max(
    candle.high -
      candle.low,

    Math.abs(
      candle.high -
      previous.close
    ),

    Math.abs(
      candle.low -
      previous.close
    )
  );
}

function averageTrueRange(
  list,
  period = 14
) {

  if (
    !list ||
    list.length < 2
  ) {
    return null;
  }

  const ranges = [];

  for (
    let i = 1;
    i < list.length;
    i++
  ) {

    ranges.push(
      trueRange(
        list[i],
        list[i - 1]
      )
    );
  }

  const start =
    Math.max(
      0,
      ranges.length -
        period
    );

  const selected =
    ranges.slice(
      start
    );

  if (
    !selected.length
  ) {
    return null;
  }

  return (
    selected.reduce(
      (
        sum,
        value
      ) =>
        sum + value,
      0
    ) /
    selected.length
  );
}

/* ============================================================
   SWING DETECTION
   ============================================================ */

function detectSwings(
  interval,
  list
) {

  const config =
    STRUCTURE[interval];

  const highs = [];

  const lows = [];

  if (
    !config ||
    !list ||
    list.length <
      config.pivotLeft +
      config.pivotRight +
      5
  ) {

    return {
      highs,
      lows
    };
  }

  const left =
    config.pivotLeft;

  const right =
    config.pivotRight;

  for (
    let i = left;
    i <
      list.length -
        right;
    i++
  ) {

    const candle =
      list[i];

    let isHigh = true;

    let isLow = true;

    /*
       Swing high.
    */
    for (
      let j =
        i - left;
      j <=
        i + right;
      j++
    ) {

      if (
        j === i
      ) {
        continue;
      }

      if (
        list[j].high >=
        candle.high
      ) {

        isHigh = false;

        break;
      }
    }

    /*
       Swing low.
    */
    for (
      let j =
        i - left;
      j <=
        i + right;
      j++
    ) {

      if (
        j === i
      ) {
        continue;
      }

      if (
        list[j].low <=
        candle.low
      ) {

        isLow = false;

        break;
      }
    }

    /*
       Reaction after swing.
    */
    let reactionCount = 0;

    const future =
      list.slice(
        i + right + 1,
        Math.min(
          list.length,
          i +
            right +
            40
        )
      );

    if (isHigh) {

      for (
        const futureCandle of
        future
      ) {

        if (
          futureCandle.low <=
          candle.high *
            (
              1 -
              REACTION_MOVE_PCT
            )
        ) {

          reactionCount++;

          break;
        }
      }

      highs.push({
        type:
          "swing_high",

        side:
          "buy_side",

        interval,

        price:
          candle.high,

        time:
          candle.openTime,

        index:
          i,

        timeframeWeight:
          config.weight,

        reactionCount
      });
    }

    if (isLow) {

      for (
        const futureCandle of
        future
      ) {

        if (
          futureCandle.high >=
          candle.low *
            (
              1 +
              REACTION_MOVE_PCT
            )
        ) {

          reactionCount++;

          break;
        }
      }

      lows.push({
        type:
          "swing_low",

        side:
          "sell_side",

        interval,

        price:
          candle.low,

        time:
          candle.openTime,

        index:
          i,

        timeframeWeight:
          config.weight,

        reactionCount
      });
    }
  }

  return {
    highs,
    lows
  };
}

/* ============================================================
   EQUAL HIGH / LOW DETECTION
   ============================================================ */

function addEqualLiquidity(
  swingList,
  tolerancePct
) {

  const result = [];

  for (
    let i = 0;
    i <
      swingList.length;
    i++
  ) {

    const base =
      swingList[i];

    let equalCount = 1;

    const matchedPrices = [
      base.price
    ];

    for (
      let j =
        0;
      j <
        swingList.length;
      j++
    ) {

      if (
        j === i
      ) {
        continue;
      }

      const other =
        swingList[j];

      const difference =
        Math.abs(
          other.price -
          base.price
        ) /
        base.price;

      if (
        difference <=
        tolerancePct
      ) {

        equalCount++;

        matchedPrices.push(
          other.price
        );
      }
    }

    const averagePrice =
      matchedPrices.reduce(
        (
          sum,
          value
        ) =>
          sum + value,
        0
      ) /
      matchedPrices.length;

    result.push({
      ...base,

      equalCount,

      averagePrice
    });
  }

  return result;
}

/* ============================================================
   MERGE STRUCTURAL LEVELS
   ============================================================ */

function mergeLevels(
  levels
) {

  if (
    !levels.length
  ) {
    return [];
  }

  const sorted =
    [...levels].sort(
      (a, b) =>
        a.price -
        b.price
    );

  const groups = [];

  for (
    const level of
    sorted
  ) {

    let group = null;

    for (
      const candidate of
      groups
    ) {

      if (
        Math.abs(
          level.price -
          candidate.price
        ) <=
        MERGE_LEVEL_DISTANCE_USD
      ) {

        group =
          candidate;

        break;
      }
    }

    if (!group) {

      group = {
        price:
          level.price,

        levels: []
      };

      groups.push(
        group
      );
    }

    group.levels.push(
      level
    );

    group.price =
      group.levels.reduce(
        (
          sum,
          x
        ) =>
          sum + x.price,
        0
      ) /
      group.levels.length;
  }

  return groups;
}

/* ============================================================
   STRUCTURAL SCORE
   ============================================================ */

function scoreStructuralGroup(
  group
) {

  const levels =
    group.levels;

  let timeframeScore = 0;

  let reactions = 0;

  let equalCount = 1;

  const intervals =
    new Set();

  for (
    const level of
    levels
  ) {

    timeframeScore +=
      level.timeframeWeight ||
      1;

    reactions +=
      level.reactionCount ||
      0;

    equalCount =
      Math.max(
        equalCount,
        level.equalCount ||
          1
      );

    intervals.add(
      level.interval
    );
  }

  const timeframeCount =
    intervals.size;

  let score = 0;

  /*
     Higher timeframe = more weight.
  */
  score +=
    timeframeScore * 10;

  /*
     Multiple reactions.
  */
  score +=
    reactions * 10;

  /*
     Equal highs/lows.
  */
  score +=
    Math.max(
      0,
      equalCount - 1
    ) * 20;

  /*
     Multi-timeframe agreement.
  */
  score +=
    Math.max(
      0,
      timeframeCount - 1
    ) * 25;

  let importance =
    "MODERATE";

  if (
    score >= 110
  ) {

    importance =
      "VERY HIGH";

  } else if (
    score >= 80
  ) {

    importance =
      "HIGH";

  } else if (
    score >= 55
  ) {

    importance =
      "MEDIUM";
  }

  return {
    score,

    importance,

    timeframeCount,

    intervals:
      [...intervals],

    reactions,

    equalCount
  };
}

/* ============================================================
   REBUILD STRUCTURE
   ============================================================ */

function rebuildStructure() {

  const allHighs = [];

  const allLows = [];

  for (
    const interval of
    Object.keys(
      STRUCTURE
    )
  ) {

    const list =
      candles[interval];

    if (
      !list ||
      !list.length
    ) {
      continue;
    }

    const result =
      detectSwings(
        interval,
        list
      );

    const highs =
      addEqualLiquidity(
        result.highs,
        STRUCTURE[interval]
          .equalTolerancePct
      );

    const lows =
      addEqualLiquidity(
        result.lows,
        STRUCTURE[interval]
          .equalTolerancePct
      );

    allHighs.push(
      ...highs
    );

    allLows.push(
      ...lows
    );
  }

  swingHighs =
    allHighs;

  swingLows =
    allLows;

  lastStructureUpdate =
    nowIso();
}

/* ============================================================
   RESTING LIQUIDITY AROUND STRUCTURAL LEVEL
   ============================================================ */

function restingLiquidityAround(
  price,
  side
) {

  const map =
    side === "sell"
      ? asks
      : bids;

  let totalUsd = 0;

  let quantity = 0;

  let strongestUsd = 0;

  let strongestPrice =
    null;

  let levelCount = 0;

  const levels = [];

  for (
    const [
      levelPrice,
      levelQuantity
    ] of map
  ) {

    const distance =
      Math.abs(
        levelPrice -
        price
      );

    if (
      distance >
      RESTING_LIQUIDITY_RADIUS_USD
    ) {
      continue;
    }

    const usd =
      usdValue(
        levelPrice,
        levelQuantity
      );

    if (
      usd <
      RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    totalUsd +=
      usd;

    quantity +=
      levelQuantity;

    levelCount++;

    if (
      usd >
      strongestUsd
    ) {

      strongestUsd =
        usd;

      strongestPrice =
        levelPrice;
    }

    levels.push({
      price:
        round(
          levelPrice,
          2
        ),

      quantity:
        round(
          levelQuantity,
          6
        ),

      usd:
        round(
          usd,
          2
        )
    });
  }

  levels.sort(
    (a, b) =>
      b.usd -
      a.usd
  );

  return {
    totalUsd,

    quantity,

    strongestUsd,

    strongestPrice,

    levelCount,

    levels
  };
}

/* ============================================================
   BUILD MAJOR LEVELS
   ============================================================ */

function buildMajorLevels() {

  if (
    !currentPrice
  ) {

    return {
      bsl: [],
      ssl: [],
      resistance: [],
      support: []
    };
  }

  /*
     Only structural highs above current price
     are BSL candidates.
  */
  const highCandidates =
    swingHighs.filter(
      level =>
        level.price >
        currentPrice
    );

  /*
     Only structural lows below current price
     are SSL candidates.
  */
  const lowCandidates =
    swingLows.filter(
      level =>
        level.price <
        currentPrice
    );

  const highGroups =
    mergeLevels(
      highCandidates
    );

  const lowGroups =
    mergeLevels(
      lowCandidates
    );

  const bsl = [];

  for (
    const group of
    highGroups
  ) {

    const score =
      scoreStructuralGroup(
        group
      );

    const resting =
      restingLiquidityAround(
        group.price,
        "sell"
      );

    let finalScore =
      score.score;

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      finalScore += 25;

    }

    /*
       Do not let raw order-book liquidity
       create a structural level.
       It can only strengthen an existing
       structural level.
    */

    if (
      finalScore <
      MIN_MAJOR_LEVEL_SCORE
    ) {
      continue;
    }

    let classification =
      "MAJOR SWING HIGH";

    if (
      score.equalCount >= 2
    ) {

      classification =
        "EQUAL HIGH / BSL";
    }

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      classification +=
        " + RESTING LIQUIDITY";
    }

    bsl.push({

      type:
        "BSL",

      side:
        "buy_side",

      price:
        round(
          group.price,
          2
        ),

      priceLow:
        round(
          group.price *
            (
              1 -
              LEVEL_ZONE_PCT
            ),
          2
        ),

      priceHigh:
        round(
          group.price *
            (
              1 +
              LEVEL_ZONE_PCT
            ),
          2
        ),

      distancePct:
        round(
          pctDistance(
            group.price,
            currentPrice
          ),
          3
        ),

      importance:
        score.importance,

      classification,

      score:
        finalScore,

      timeframeCount:
        score.timeframeCount,

      intervals:
        score.intervals,

      equalCount:
        score.equalCount,

      reactionCount:
        score.reactions,

      restingSellUsd:
        round(
          resting.totalUsd,
          2
        ),

      strongestRestingSellUsd:
        round(
          resting.strongestUsd,
          2
        ),

      strongestRestingSellPrice:
        resting.strongestPrice,

      restingLevels:
        resting.levelCount
    });
  }

  const ssl = [];

  for (
    const group of
    lowGroups
  ) {

    const score =
      scoreStructuralGroup(
        group
      );

    const resting =
      restingLiquidityAround(
        group.price,
        "buy"
      );

    let finalScore =
      score.score;

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      finalScore += 25;
    }

    if (
      finalScore <
      MIN_MAJOR_LEVEL_SCORE
    ) {
      continue;
    }

    let classification =
      "MAJOR SWING LOW";

    if (
      score.equalCount >= 2
    ) {

      classification =
        "EQUAL LOW / SSL";
    }

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      classification +=
        " + RESTING LIQUIDITY";
    }

    ssl.push({

      type:
        "SSL",

      side:
        "sell_side",

      price:
        round(
          group.price,
          2
        ),

      priceLow:
        round(
          group.price *
            (
              1 -
              LEVEL_ZONE_PCT
            ),
          2
        ),

      priceHigh:
        round(
          group.price *
            (
              1 +
              LEVEL_ZONE_PCT
            ),
          2
        ),

      distancePct:
        round(
          pctDistance(
            group.price,
            currentPrice
          ),
          3
        ),

      importance:
        score.importance,

      classification,

      score:
        finalScore,

      timeframeCount:
        score.timeframeCount,

      intervals:
        score.intervals,

      equalCount:
        score.equalCount,

      reactionCount:
        score.reactions,

      restingBuyUsd:
        round(
          resting.totalUsd,
          2
        ),

      strongestRestingBuyUsd:
        round(
          resting.strongestUsd,
          2
        ),

      strongestRestingBuyPrice:
        resting.strongestPrice,

      restingLevels:
        resting.levelCount
    });
  }

  /*
     Sort by structural importance.
  */
  const importantBSL =
    bsl
      .sort(
        (a, b) =>
          b.score -
          a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  const importantSSL =
    ssl
      .sort(
        (a, b) =>
          b.score -
          a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  /*
     Resistance:
     nearest BSL first.
  */
  const resistance =
    [...importantBSL]
      .sort(
        (a, b) =>
          a.price -
          b.price
      );

  /*
     Support:
     nearest SSL first.
  */
  const support =
    [...importantSSL]
      .sort(
        (a, b) =>
          b.price -
          a.price
      );

  return {
    bsl:
      importantBSL,

    ssl:
      importantSSL,

    resistance,

    support
  };
}

/* ============================================================
   RAW RESTING ORDER BOOK
   ============================================================ */

function buildRawBookLiquidity() {

  if (
    !currentPrice
  ) {

    return {
      largeBids: [],
      largeAsks: []
    };
  }

  const lower =
    currentPrice *
    (
      1 -
      RANGE_PCT
    );

  const upper =
    currentPrice *
    (
      1 +
      RANGE_PCT
    );

  const largeBids = [];

  for (
    const [
      price,
      quantity
    ] of bids
  ) {

    if (
      price <
        lower ||
      price >
        currentPrice
    ) {
      continue;
    }

    const usd =
      usdValue(
        price,
        quantity
      );

    if (
      usd <
      RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    largeBids.push({

      price:
        round(
          price,
          2
        ),

      quantity:
        round(
          quantity,
          6
        ),

      usd:
        round(
          usd,
          2
        )
    });
  }

  const largeAsks = [];

  for (
    const [
      price,
      quantity
    ] of asks
  ) {

    if (
      price >
        upper ||
      price <
        currentPrice
    ) {
      continue;
    }

    const usd =
      usdValue(
        price,
        quantity
      );

    if (
      usd <
      RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    largeAsks.push({

      price:
        round(
          price,
          2
        ),

      quantity:
        round(
          quantity,
          6
        ),

      usd:
        round(
          usd,
          2
        )
    });
  }

  largeBids.sort(
    (a, b) =>
      b.usd -
      a.usd
  );

  largeAsks.sort(
    (a, b) =>
      b.usd -
      a.usd
  );

  return {

    largeBids:
      largeBids.slice(
        0,
        50
      ),

    largeAsks:
      largeAsks.slice(
        0,
        50
      )
  };
}

/* ============================================================
   LIQUIDITY MAP
   ============================================================ */

function buildLiquidityMap() {

  const bookMid =
    getBookMid();

  if (
    bookMid
  ) {
    currentPrice =
      bookMid;
  }

  const majorLevels =
    buildMajorLevels();

  const rawBook =
    buildRawBookLiquidity();

  const nearestBSL =
    [...majorLevels.bsl]
      .sort(
        (a, b) =>
          a.price -
          b.price
      )
      .slice(
        0,
        3
      );

  const nearestSSL =
    [...majorLevels.ssl]
      .sort(
        (a, b) =>
          b.price -
          a.price
      )
      .slice(
        0,
        3
      );

  return {

    symbol:
      SYMBOL,

    currentPrice:
      currentPrice
        ? round(
            currentPrice,
            2
          )
        : null,

    /*
       THIS IS NOW THE PRIMARY OUTPUT.
    */
    marketStructure: {

      buySideLiquidity:
        majorLevels.bsl,

      sellSideLiquidity:
        majorLevels.ssl,

      majorResistance:
        majorLevels.resistance,

      majorSupport:
        majorLevels.support,

      nearestBSL,

      nearestSSL
    },

    /*
       This remains available for diagnostics,
       but should NOT be interpreted as major
       support/resistance by itself.
    */
    rawRestingLiquidity: {

      largeBids:
        rawBook.largeBids,

      largeAsks:
        rawBook.largeAsks
    },

    settings: {

      structureTimeframes:
        Object.keys(
          STRUCTURE
        ),

      levelZonePct:
        LEVEL_ZONE_PCT,

      equalHighLowTolerance:
        "timeframe dependent",

      minMajorLevelScore:
        MIN_MAJOR_LEVEL_SCORE,

      minStructuralRestingUsd:
        MIN_STRUCTURAL_RESTING_USD,

      restingLiquidityRadiusUsd:
        RESTING_LIQUIDITY_RADIUS_USD,

      mergeLevelDistanceUsd:
        MERGE_LEVEL_DISTANCE_USD
    },

    structureStats: {

      candles15m:
        candles["15m"].length,

      candles1h:
        candles["1h"].length,

      candles4h:
        candles["4h"].length,

      candles1d:
        candles["1d"].length,

      swingHighs:
        swingHighs.length,

      swingLows:
        swingLows.length,

      updatedAt:
        lastStructureUpdate
    },

    book: {

      initialized,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      lastUpdateId
    },

    sync: {

      status:
        initialized
          ? (
              gapRecovery
                ? "gap_recovery"
                : "live"
            )
          : "syncing",

      gapRecovery,

      gapDetected,

      gapCount,

      lastGapLocal,

      lastGapIncoming,

      pendingEvents:
        pendingDepthEvents.length,

      snapshotPending,

      snapshotId:
        lastSnapshotId
    },

    connections: {

      depthWebSocket:
        Boolean(
          depthWs &&
          depthWs.readyState ===
            WebSocket.OPEN
        ),

      snapshotWebSocket:
        Boolean(
          snapshotWs &&
          snapshotWs.readyState ===
            WebSocket.OPEN
        ),

      klineWebSocket:
        Boolean(
          klineWs &&
          klineWs.readyState ===
            WebSocket.OPEN
        )
    },

    updatedAt:
      nowIso()
  };
}

/* ============================================================
   HEALTH
   ============================================================ */

app.get(
  "/health",
  (req, res) => {

    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      status:
        initialized
          ? (
              gapRecovery
                ? "gap_recovery"
                : "live"
            )
          : "syncing",

      initialized,

      waitingForBridge,

      snapshotPending,

      depthConnected:
        Boolean(
          depthWs &&
          depthWs.readyState ===
            WebSocket.OPEN
        ),

      snapshotConnected:
        Boolean(
          snapshotWs &&
          snapshotWs.readyState ===
            WebSocket.OPEN
        ),

      klineConnected:
        Boolean(
          klineWs &&
          klineWs.readyState ===
            WebSocket.OPEN
        ),

      currentPrice,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      pendingEvents:
        pendingDepthEvents.length,

      lastUpdateId,

      snapshotRequests,

      snapshot429s,

      bridgeAttempts,

      bridgeFound,

      gapCount,

      lastGapLocal,

      lastGapIncoming,

      lastSnapshotRequest,

      lastSnapshotResponse,

      lastSnapshotError,

      lastSnapshotId,

      structure: {

        candles15m:
          candles["15m"].length,

        candles1h:
          candles["1h"].length,

        candles4h:
          candles["4h"].length,

        candles1d:
          candles["1d"].length,

        swingHighs:
          swingHighs.length,

        swingLows:
          swingLows.length,

        updatedAt:
          lastStructureUpdate
      },

      updatedAt:
        nowIso()
    });
  }
);

/* ============================================================
   LIQUIDITY ENDPOINT
   ============================================================ */

app.get(
  "/liquidity",
  (req, res) => {

    res.json(
      buildLiquidityMap()
    );
  }
);

/* ============================================================
   BOOK ENDPOINT
   ============================================================ */

app.get(
  "/book",
  (req, res) => {

    const bidLevels =
      [...bids.entries()]
        .sort(
          (a, b) =>
            b[0] -
            a[0]
        )
        .slice(
          0,
          100
        );

    const askLevels =
      [...asks.entries()]
        .sort(
          (a, b) =>
            a[0] -
            b[0]
        )
        .slice(
          0,
          100
        );

    res.json({

      symbol:
        SYMBOL,

      currentPrice:
        currentPrice
          ? round(
              currentPrice,
              2
            )
          : null,

      bids:
        bidLevels.map(
          ([
            price,
            quantity
          ]) => ({

            price,

            quantity,

            usd:
              round(
                usdValue(
                  price,
                  quantity
                ),
                2
              )
          })
        ),

      asks:
        askLevels.map(
          ([
            price,
            quantity
          ]) => ({

            price,

            quantity,

            usd:
              round(
                usdValue(
                  price,
                  quantity
                ),
                2
              )
          })
        ),

      initialized,

      lastUpdateId,

      updatedAt:
        nowIso()
    });
  }
);

/* ============================================================
   ROOT
   ============================================================ */

app.get(
  "/",
  (req, res) => {

    res.json({

      ok:
        true,

      service:
        "Binance Futures Major Liquidity Relay",

      symbol:
        SYMBOL,

      endpoints: [
        "/health",
        "/liquidity",
        "/book"
      ],

      primaryEngine:
        "Market Structure + BSL/SSL",

      secondaryEngine:
        "Resting Order Book Liquidity"
    });
  }
);

/* ============================================================
   START SERVER
   ============================================================ */

app.listen(
  PORT,
  () => {

    console.log(
      "================================================"
    );

    console.log(
      "BINANCE FUTURES MAJOR LIQUIDITY RELAY"
    );

    console.log(
      "================================================"
    );

    console.log(
      `Symbol: ${SYMBOL}`
    );

    console.log(
      `Port: ${PORT}`
    );

    console.log(
      "Structure: 15m / 1h / 4h / 1D"
    );

    console.log(
      "Primary: Swing High/Low + Equal High/Low"
    );

    console.log(
      "Secondary: Resting Bids/Asks"
    );

    console.log(
      "================================================"
    );
  }
);

/* ============================================================
   START CONNECTIONS
   ============================================================ */

connectDepthWebSocket();

connectKlineWebSocket();

setTimeout(
  () => {
    connectSnapshotWebSocket();
  },
  500
);
