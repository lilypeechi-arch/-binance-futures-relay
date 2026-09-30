const express = require("express");
const WebSocket = require("ws");
const https = require("https");

const app = express();

const PORT = process.env.PORT || 10000;

const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

/* ============================================================
   BINANCE FUTURES
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

const BINANCE_FUTURES_REST =
  "https://fapi.binance.com";

/* ============================================================
   BYBIT HISTORICAL FUTURES
   ============================================================ */

const BYBIT_REST =
  "https://api.bybit.com";

/*
   Bybit linear Futures interval mapping

   Binance:
   15m
   1h
   4h
   1d

   Bybit:
   15
   60
   240
   D
*/

const BYBIT_INTERVALS = {
  "15m": "15",
  "1h": "60",
  "4h": "240",
  "1d": "D"
};

/* ============================================================
   GENERAL SETTINGS
   ============================================================ */

const MAX_PENDING_EVENTS = 2500;

const SNAPSHOT_MIN_INTERVAL_MS = 65000;
const SNAPSHOT_TIMEOUT_MS = 15000;

const RAW_BOOK_MIN_USD = 50000;

const RESTING_LIQUIDITY_RADIUS_USD = 35;

const MIN_STRUCTURAL_RESTING_USD = 250000;

const MERGE_LEVEL_DISTANCE_USD = 35;

const LEVEL_ZONE_PCT = 0.0015;

const REACTION_MOVE_PCT = 0.003;

const MIN_MAJOR_LEVEL_SCORE = 40;

const MAX_MAJOR_LEVELS = 8;

/* ============================================================
   MARKET STRUCTURE SETTINGS
   ============================================================ */

const STRUCTURE = {

  "15m": {

    interval:
      "15m",

    weight:
      1,

    candles:
      300,

    pivotLeft:
      3,

    pivotRight:
      3,

    equalTolerancePct:
      0.0015
  },

  "1h": {

    interval:
      "1h",

    weight:
      2,

    candles:
      300,

    pivotLeft:
      4,

    pivotRight:
      4,

    equalTolerancePct:
      0.0015
  },

  "4h": {

    interval:
      "4h",

    weight:
      3,

    candles:
      250,

    pivotLeft:
      5,

    pivotRight:
      5,

    equalTolerancePct:
      0.002
  },

  "1d": {

    interval:
      "1d",

    weight:
      4,

    candles:
      180,

    pivotLeft:
      5,

    pivotRight:
      5,

    equalTolerancePct:
      0.0025
  }
};

/* ============================================================
   ORDER BOOK
   ============================================================ */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

let depthWs = null;
let snapshotWs = null;
let klineWs = null;

/* ============================================================
   ORDER BOOK SYNC STATE
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

/* ============================================================
   HISTORICAL CANDLES
   ============================================================ */

const candles = {

  "15m": [],

  "1h": [],

  "4h": [],

  "1d": []
};

let candleRequests = 0;

let candleSuccess = 0;

let candleErrors = 0;

let candleLastError = null;

let candleLastStatus = null;

let candleLastSource = null;

let candleUpdatedAt = null;

/* ============================================================
   STRUCTURE
   ============================================================ */

let swingHighs = [];

let swingLows = [];

let lastStructureUpdate = null;

/* ============================================================
   UTILITY
   ============================================================ */

function nowIso() {

  return new Date().toISOString();

}

function safeNumber(value) {

  const n =
    Number(value);

  return Number.isFinite(n)
    ? n
    : null;

}

function round(
  value,
  decimals = 2
) {

  const p =
    Math.pow(
      10,
      decimals
    );

  return Math.round(
    value * p
  ) / p;

}

function pctDistance(
  price,
  reference
) {

  if (
    reference === null ||
    reference === undefined ||
    reference === 0
  ) {

    return 0;

  }

  return (
    (
      (price - reference) /
      reference
    ) *
    100
  );

}

function usdValue(
  price,
  quantity
) {

  return (
    price *
    quantity
  );

}

/* ============================================================
   HTTPS JSON GET
   ============================================================ */

function httpsGetJson(
  url,
  timeoutMs = 20000
) {

  return new Promise(
    (
      resolve,
      reject
    ) => {

      const request =
        https.get(
          url,
          {
            headers: {
              "User-Agent":
                "Mozilla/5.0 Binance-Futures-Relay"
            }
          },
          response => {

            let body =
              "";

            response.on(
              "data",
              chunk => {

                body +=
                  chunk;

              }
            );

            response.on(
              "end",
              () => {

                const status =
                  response.statusCode;

                if (
                  status < 200 ||
                  status >= 300
                ) {

                  const error =
                    new Error(
                      `HTTP ${status}: ${body.slice(
                        0,
                        500
                      )}`
                    );

                  error.statusCode =
                    status;

                  reject(
                    error
                  );

                  return;
                }

                try {

                  const json =
                    JSON.parse(
                      body
                    );

                  resolve(
                    json
                  );

                } catch (error) {

                  const parseError =
                    new Error(
                      `Invalid JSON: ${body.slice(
                        0,
                        500
                      )}`
                    );

                  parseError.statusCode =
                    status;

                  reject(
                    parseError
                  );

                }

              }
            );

          }
        );

      request.setTimeout(
        timeoutMs,
        () => {

          request.destroy();

          reject(
            new Error(
              "HTTP request timeout"
            )
          );

        }
      );

      request.on(
        "error",
        error => {

          reject(
            error
          );

        }
      );

    }
  );

}

/* ============================================================
   BYBIT HISTORICAL KLINES
   ============================================================ */

async function loadBybitHistoricalKlines(
  interval
) {

  const config =
    STRUCTURE[interval];

  if (!config) {

    throw new Error(
      `Unknown structure interval: ${interval}`
    );

  }

  const bybitInterval =
    BYBIT_INTERVALS[interval];

  const limit =
    config.candles;

  const url =
    `${BYBIT_REST}/v5/market/kline` +
    `?category=linear` +
    `&symbol=${SYMBOL}` +
    `&interval=${bybitInterval}` +
    `&limit=${limit}`;

  console.log(
    `Loading Bybit ${interval} historical candles...`
  );

  const result =
    await httpsGetJson(
      url,
      20000
    );

  if (
    !result ||
    result.retCode !== 0
  ) {

    throw new Error(
      `Bybit error: ${
        result?.retMsg ||
        "Unknown error"
      }`
    );

  }

  if (
    !result.result ||
    !Array.isArray(
      result.result.list
    )
  ) {

    throw new Error(
      "Bybit returned no kline list"
    );

  }

  /*
     Bybit returns newest first.

     We reverse it so the rest of
     the structure engine receives
     oldest -> newest.
  */

  const rows =
    [
      ...result.result.list
    ].reverse();

  const parsed =
    rows
      .map(
        row => {

          /*
             Bybit format:

             [
               startTime,
               openPrice,
               highPrice,
               lowPrice,
               closePrice,
               volume,
               turnover
             ]
          */

          const openTime =
            Number(
              row[0]
            );

          const open =
            Number(
              row[1]
            );

          const high =
            Number(
              row[2]
            );

          const low =
            Number(
              row[3]
            );

          const close =
            Number(
              row[4]
            );

          const volume =
            Number(
              row[5]
            );

          if (
            !Number.isFinite(
              openTime
            ) ||
            !Number.isFinite(
              open
            ) ||
            !Number.isFinite(
              high
            ) ||
            !Number.isFinite(
              low
            ) ||
            !Number.isFinite(
              close
            )
          ) {

            return null;

          }

          return {

            openTime,

            open,

            high,

            low,

            close,

            volume:
              Number.isFinite(
                volume
              )
                ? volume
                : 0,

            closeTime:
              null,

            closed:
              true,

            source:
              "Bybit"
          };

        }
      )
      .filter(
        Boolean
      );

  if (
    !parsed.length
  ) {

    throw new Error(
      "Bybit returned zero valid candles"
    );

  }

  candles[interval] =
    parsed;

  candleSuccess++;

  candleLastStatus =
    200;

  candleLastSource =
    "Bybit";

  candleLastError =
    null;

  candleUpdatedAt =
    nowIso();

  console.log(
    `Loaded ${parsed.length} Bybit ${interval} candles`
  );

  return true;

}

/* ============================================================
   BINANCE HISTORICAL FALLBACK
   ============================================================ */

async function loadBinanceHistoricalKlines(
  interval
) {

  const config =
    STRUCTURE[interval];

  const url =
    `${BINANCE_FUTURES_REST}/fapi/v1/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${config.candles}`;

  console.log(
    `Trying Binance ${interval} historical candles...`
  );

  const result =
    await httpsGetJson(
      url,
      20000
    );

  if (
    !Array.isArray(
      result
    )
  ) {

    throw new Error(
      "Binance returned invalid kline response"
    );

  }

  const parsed =
    result
      .map(
        row => ({

          openTime:
            Number(
              row[0]
            ),

          open:
            Number(
              row[1]
            ),

          high:
            Number(
              row[2]
            ),

          low:
            Number(
              row[3]
            ),

          close:
            Number(
              row[4]
            ),

          volume:
            Number(
              row[5]
            ),

          closeTime:
            Number(
              row[6]
            ),

          closed:
            true,

          source:
            "Binance"
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

  if (
    !parsed.length
  ) {

    throw new Error(
      "Binance returned zero valid candles"
    );

  }

  candles[interval] =
    parsed;

  candleSuccess++;

  candleLastStatus =
    200;

  candleLastSource =
    "Binance";

  candleLastError =
    null;

  candleUpdatedAt =
    nowIso();

  console.log(
    `Loaded ${parsed.length} Binance ${interval} candles`
  );

  return true;

}

/* ============================================================
   LOAD HISTORICAL CANDLES
   ============================================================ */

async function loadHistoricalKlines(
  interval
) {

  candleRequests++;

  /*
     IMPORTANT:

     Bybit is tried first because
     Binance Futures REST is blocked
     from the current Render location.
  */

  try {

    await loadBybitHistoricalKlines(
      interval
    );

    rebuildStructure();

    return true;

  } catch (bybitError) {

    console.error(
      `Bybit ${interval} error:`,
      bybitError.message
    );

    /*
       Binance fallback is retained
       in case Binance REST becomes
       available later.
    */

    try {

      await loadBinanceHistoricalKlines(
        interval
      );

      rebuildStructure();

      return true;

    } catch (binanceError) {

      candleErrors++;

      candleLastStatus =
        binanceError.statusCode ||
        bybitError.statusCode ||
        null;

      candleLastError =
        `Bybit: ${bybitError.message} | ` +
        `Binance: ${binanceError.message}`;

      console.error(
        `Historical ${interval} failed:`,
        candleLastError
      );

      return false;

    }

  }

}

/* ============================================================
   LOAD ALL HISTORICAL STRUCTURE
   ============================================================ */

async function loadAllHistoricalKlines() {

  console.log(
    "================================================"
  );

  console.log(
    "LOADING HISTORICAL STRUCTURE DATA"
  );

  console.log(
    "================================================"
  );

  /*
     Sequential requests.

     This prevents unnecessary
     rate-limit pressure.
  */

  await loadHistoricalKlines(
    "15m"
  );

  await loadHistoricalKlines(
    "1h"
  );

  await loadHistoricalKlines(
    "4h"
  );

  await loadHistoricalKlines(
    "1d"
  );

  rebuildStructure();

  console.log(
    "================================================"
  );

  console.log(
    "HISTORICAL STRUCTURE LOADING COMPLETE"
  );

  console.log(
    `15m candles: ${candles["15m"].length}`
  );

  console.log(
    `1h candles: ${candles["1h"].length}`
  );

  console.log(
    `4h candles: ${candles["4h"].length}`
  );

  console.log(
    `1d candles: ${candles["1d"].length}`
  );

  console.log(
    `Swing highs: ${swingHighs.length}`
  );

  console.log(
    `Swing lows: ${swingLows.length}`
  );

  console.log(
    "================================================"
  );

}

/* ============================================================
   ORDER BOOK FUNCTIONS
   ============================================================ */

function applyBookSide(
  map,
  updates
) {

  if (
    !Array.isArray(
      updates
    )
  ) {

    return;

  }

  for (
    const item of updates
  ) {

    if (
      !Array.isArray(item) ||
      item.length < 2
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
      price === null ||
      quantity === null
    ) {

      continue;

    }

    if (
      quantity === 0
    ) {

      map.delete(
        price
      );

    } else {

      map.set(
        price,
        quantity
      );

    }

  }

}

function applyDepthEvent(
  event
) {

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
      Number(
        event.u
      );

  }

}

function getBestBid() {

  let best =
    null;

  for (
    const [
      price,
      quantity
    ] of bids
  ) {

    if (
      quantity <= 0
    ) {

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

  let best =
    null;

  for (
    const [
      price,
      quantity
    ] of asks
  ) {

    if (
      quantity <= 0
    ) {

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

function bufferDepthEvent(
  event
) {

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
    Number(
      snapshotId
    ) + 1;

  for (
    const event of
    pendingDepthEvents
  ) {

    const U =
      Number(
        event.U
      );

    const u =
      Number(
        event.u
      );

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

  applyDepthEvent(
    bridge
  );

  initialized =
    true;

  waitingForBridge =
    false;

  snapshotPending =
    false;

  gapRecovery =
    false;

  gapDetected =
    false;

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

    pendingDepthEvents =
      [];

  }

  return true;

}

/* ============================================================
   SNAPSHOT REQUEST CONTROL
   ============================================================ */

function canRequestSnapshot() {

  if (
    snapshotPending
  ) {

    return false;

  }

  if (
    !lastSnapshotRequest
  ) {

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

function requestSnapshot() {

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

  snapshotPending =
    true;

  snapshotRequests++;

  lastSnapshotRequest =
    nowIso();

  const id =
    String(
      ++snapshotRequestId
    );

  const request = {

    id,

    method:
      "depth",

    params: {

      symbol:
        SYMBOL,

      limit:
        1000

    }

  };

  try {

    snapshotWs.send(
      JSON.stringify(
        request
      )
    );

  } catch (error) {

    snapshotPending =
      false;

    lastSnapshotError =
      error.message;

    return;

  }

  if (
    snapshotTimeout
  ) {

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
   INSTALL SNAPSHOT
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

  bids.clear();

  asks.clear();

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

  initialized =
    false;

  waitingForBridge =
    true;

  gapRecovery =
    false;

  gapDetected =
    false;

  lastSnapshotResponse =
    nowIso();

  lastSnapshotError =
    null;

  if (
    snapshotTimeout
  ) {

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
   HANDLE DEPTH
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

  bufferDepthEvent(
    event
  );

  if (
    !initialized
  ) {

    tryCompleteBridge();

    return;

  }

  const U =
    Number(
      event.U
    );

  const u =
    Number(
      event.u
    );

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {

    return;

  }

  if (
    U <=
      lastUpdateId + 1 &&
    u >=
      lastUpdateId + 1
  ) {

    applyDepthEvent(
      event
    );

    if (
      gapRecovery
    ) {

      gapRecovery =
        false;

      gapDetected =
        false;

    }

    while (
      pendingDepthEvents.length >
      0
    ) {

      const first =
        pendingDepthEvents[0];

      if (
        Number(
          first.u
        ) <=
        lastUpdateId
      ) {

        pendingDepthEvents.shift();

      } else {

        break;

      }

    }

    return;

  }

  if (
    u <=
    lastUpdateId
  ) {

    return;

  }

  if (
    gapRecovery
  ) {

    return;

  }

  gapRecovery =
    true;

  gapDetected =
    true;

  gapCount++;

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

  if (
    snapshotTimer
  ) {

    return;

  }

  let delay =
    1000;

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

        requestSnapshot();

      },
      delay
    );

}

/* ============================================================
   BINANCE DEPTH WEBSOCKET
   ============================================================ */

function connectDepthWebSocket() {

  if (
    depthWs
  ) {

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
   BINANCE SNAPSHOT WEBSOCKET
   ============================================================ */

function connectSnapshotWebSocket() {

  if (
    snapshotWs
  ) {

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

      requestSnapshot();

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

        if (
          message.status ===
            200 &&
          message.result &&
          message.result.lastUpdateId !==
            undefined
        ) {

          installSnapshot(
            message.result
          );

          return;

        }

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
   BINANCE LIVE KLINE WEBSOCKET
   ============================================================ */

function connectKlineWebSocket() {

  if (
    klineWs
  ) {

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
            Boolean(k.x),

          source:
            "Binance"
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
   LIVE CANDLE UPDATE
   ============================================================ */

function upsertCandle(
  interval,
  candle
) {

  if (
    !candles[interval]
  ) {

    candles[interval] =
      [];

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
    STRUCTURE[interval]?.candles ||
    300;

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
     Rebuild only when candle closes.
  */

  if (
    candle.closed
  ) {

    rebuildStructure();

  }

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

  const highs =
    [];

  const lows =
    [];

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

    let isHigh =
      true;

    let isLow =
      true;

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

        isHigh =
          false;

        break;

      }

    }

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

        isLow =
          false;

        break;

      }

    }

    let highReaction =
      0;

    let lowReaction =
      0;

    const future =
      list.slice(
        i +
          right +
          1,
        Math.min(
          list.length,
          i +
            right +
            40
        )
      );

    if (
      isHigh
    ) {

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

          highReaction =
            1;

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

        reactionCount:
          highReaction,

        timeframeWeight:
          config.weight
      });

    }

    if (
      isLow
    ) {

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

          lowReaction =
            1;

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

        reactionCount:
          lowReaction,

        timeframeWeight:
          config.weight
      });

    }

  }

  return {
    highs,
    lows
  };

}

/* ============================================================
   EQUAL HIGHS / LOWS
   ============================================================ */

function addEqualLiquidity(
  swingList,
  tolerancePct
) {

  const result =
    [];

  for (
    let i = 0;
    i <
      swingList.length;
    i++
  ) {

    const base =
      swingList[i];

    let equalCount =
      1;

    for (
      let j = 0;
      j <
        swingList.length;
      j++
    ) {

      if (
        i === j
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

      }

    }

    result.push({

      ...base,

      equalCount

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

  const groups =
    [];

  for (
    const level of
    sorted
  ) {

    let group =
      null;

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

        levels:
          []

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
          sum +
          x.price,
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

  let timeframeScore =
    0;

  let reactions =
    0;

  let equalCount =
    1;

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

  let score =
    0;

  /*
     Higher timeframe
     = higher weight.
  */

  score +=
    timeframeScore *
    10;

  /*
     Reactions
     = evidence that price
     respected the level.
  */

  score +=
    reactions *
    10;

  /*
     Equal highs/lows
     = additional liquidity.
  */

  score +=
    Math.max(
      0,
      equalCount - 1
    ) *
    20;

  /*
     Multi-timeframe agreement
     = stronger structure.
  */

  score +=
    Math.max(
      0,
      timeframeCount - 1
    ) *
    25;

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

  const allHighs =
    [];

  const allLows =
    [];

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
        STRUCTURE[
          interval
        ].equalTolerancePct
      );

    const lows =
      addEqualLiquidity(
        result.lows,
        STRUCTURE[
          interval
        ].equalTolerancePct
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

  /*
     SELL = Binance asks
     BUY  = Binance bids
  */

  const map =
    side === "sell"
      ? asks
      : bids;

  let totalUsd =
    0;

  let strongestUsd =
    0;

  let strongestPrice =
    null;

  let levelCount =
    0;

  for (
    const [
      levelPrice,
      levelQuantity
    ] of map
  ) {

    if (
      Math.abs(
        levelPrice -
        price
      ) >
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

  }

  return {

    totalUsd,

    strongestUsd,

    strongestPrice,

    levelCount

  };

}

/* ============================================================
   BUILD MAJOR STRUCTURAL LEVELS
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
     BSL must be ABOVE current price.
  */

  const highCandidates =
    swingHighs.filter(
      level =>
        level.price >
        currentPrice
    );

  /*
     SSL must be BELOW current price.
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

  const bsl =
    [];

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

    /*
       Resting Binance asks
       confirm the structural
       BSL level.
    */

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      finalScore +=
        25;

    }

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

    bsl.push({

      type:
        "BSL",

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

      classification,

      importance:
        score.importance,

      score:
        finalScore,

      intervals:
        score.intervals,

      timeframeCount:
        score.timeframeCount,

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
        resting.strongestPrice

    });

  }

  const ssl =
    [];

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

    /*
       Resting Binance bids
       confirm the structural
       SSL level.
    */

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {

      finalScore +=
        25;

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

    ssl.push({

      type:
        "SSL",

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

      classification,

      importance:
        score.importance,

      score:
        finalScore,

      intervals:
        score.intervals,

      timeframeCount:
        score.timeframeCount,

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
        resting.strongestPrice

    });

  }

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
     nearest BSL levels first.
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
     nearest SSL levels first.
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
   RAW BINANCE ORDER BOOK
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
    0.95;

  const upper =
    currentPrice *
    1.05;

  const largeBids =
    [];

  const largeAsks =
    [];

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
   COMPLETE LIQUIDITY MAP
   ============================================================ */

function buildLiquidityMap() {

  const mid =
    getBookMid();

  if (mid) {

    currentPrice =
      mid;

  }

  const structure =
    buildMajorLevels();

  const raw =
    buildRawBookLiquidity();

  const nearestBSL =
    [...structure.bsl]
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
    [...structure.ssl]
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

    marketStructure: {

      buySideLiquidity:
        structure.bsl,

      sellSideLiquidity:
        structure.ssl,

      majorResistance:
        structure.resistance,

      majorSupport:
        structure.support,

      nearestBSL,

      nearestSSL

    },

    rawRestingLiquidity: {

      largeBids:
        raw.largeBids,

      largeAsks:
        raw.largeAsks

    },

    settings: {

      structureTimeframes:
        Object.keys(
          STRUCTURE
        ),

      historicalStructureSource:
        candleLastSource ||
        "none",

      minMajorLevelScore:
        MIN_MAJOR_LEVEL_SCORE,

      mergeLevelDistanceUsd:
        MERGE_LEVEL_DISTANCE_USD,

      restingLiquidityRadiusUsd:
        RESTING_LIQUIDITY_RADIUS_USD,

      minStructuralRestingUsd:
        MIN_STRUCTURAL_RESTING_USD

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

    candleHistory: {

      requests:
        candleRequests,

      successful:
        candleSuccess,

      errors:
        candleErrors,

      lastStatus:
        candleLastStatus,

      lastSource:
        candleLastSource,

      lastError:
        candleLastError,

      updatedAt:
        candleUpdatedAt

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

      currentPrice,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

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

      candles: {

        "15m":
          candles["15m"].length,

        "1h":
          candles["1h"].length,

        "4h":
          candles["4h"].length,

        "1d":
          candles["1d"].length

      },

      candleHistory: {

        requests:
          candleRequests,

        successful:
          candleSuccess,

        errors:
          candleErrors,

        source:
          candleLastSource,

        lastStatus:
          candleLastStatus,

        lastError:
          candleLastError,

        updatedAt:
          candleUpdatedAt

      },

      structure: {

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
   DIAGNOSTIC
   ============================================================ */

app.get(
  "/diagnostic",
  (req, res) => {

    res.json({

      service:
        "Binance Futures Major Liquidity Relay",

      symbol:
        SYMBOL,

      historicalStructureSource:
        "Bybit Linear Futures",

      bybitRest:
        BYBIT_REST,

      bybitKlines:
        `${BYBIT_REST}/v5/market/kline`,

      binanceFuturesRest:
        BINANCE_FUTURES_REST,

      binanceFuturesKlines:
        `${BINANCE_FUTURES_REST}/fapi/v1/klines`,

      candleHistory: {

        "15m":
          candles["15m"].length,

        "1h":
          candles["1h"].length,

        "4h":
          candles["4h"].length,

        "1d":
          candles["1d"].length

      },

      candleRequests:
        candleRequests,

      candleSuccess:
        candleSuccess,

      candleErrors:
        candleErrors,

      candleLastSource:
        candleLastSource,

      candleLastStatus:
        candleLastStatus,

      candleLastError:
        candleLastError,

      candleUpdatedAt:
        candleUpdatedAt,

      structure: {

        swingHighs:
          swingHighs.length,

        swingLows:
          swingLows.length,

        updatedAt:
          lastStructureUpdate

      },

      orderBook: {

        initialized,

        bids:
          bids.size,

        asks:
          asks.size

      },

      connections: {

        depth:
          Boolean(
            depthWs &&
            depthWs.readyState ===
              WebSocket.OPEN
          ),

        snapshot:
          Boolean(
            snapshotWs &&
            snapshotWs.readyState ===
              WebSocket.OPEN
          ),

        klines:
          Boolean(
            klineWs &&
            klineWs.readyState ===
              WebSocket.OPEN
          )

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
   RAW BOOK ENDPOINT
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

        "/diagnostic",

        "/liquidity",

        "/book"

      ],

      architecture: {

        liveOrderBook:
          "Binance Futures WebSocket",

        liveCandles:
          "Binance Futures WebSocket",

        historicalCandles:
          "Bybit Linear Futures REST",

        structure:
          "15m / 1h / 4h / 1D",

        liquidity:
          "Binance Futures Order Book"

      },

      primary:
        "Market Structure BSL/SSL",

      secondary:
        "Binance Resting Order Book Liquidity"

    });

  }
);

/* ============================================================
   SERVER START
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
      "Live order book: Binance Futures"
    );

    console.log(
      "Live candles: Binance Futures"
    );

    console.log(
      "Historical candles: Bybit Linear Futures"
    );

    console.log(
      "Structure: 15m / 1h / 4h / 1D"
    );

    console.log(
      "BSL / SSL: Structural"
    );

    console.log(
      "Resting liquidity: Binance only"
    );

    console.log(
      "================================================"
    );

    setTimeout(
      () => {

        loadAllHistoricalKlines()
          .catch(
            error => {

              console.error(
                "Historical structure loader:",
                error.message
              );

            }
          );

      },
      2000
    );

  }
);

/* ============================================================
   START CONNECTIONS
   ============================================================ */

connectDepthWebSocket();

connectSnapshotWebSocket();

connectKlineWebSocket();
