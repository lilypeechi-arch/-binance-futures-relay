/* ============================================================
   BINANCE BTCUSDT LIQUIDITY / MARKET STRUCTURE RELAY
   V10 - STABLE SNAPSHOT + CONCENTRATED WALL DETECTOR

   DATA SOURCES
   ------------------------------------------------------------
   Historical structure:
     Binance Spot data-api.binance.vision

   Live order book:
     Binance Futures depth websocket

   Live candles:
     Binance Futures kline websocket

   Snapshot:
     Binance Futures WebSocket API

   IMPORTANT
   ------------------------------------------------------------
   BSL = structural highs ABOVE current price
   SSL = structural lows BELOW current price

   Resting bids/asks are kept separate from structural liquidity.
   ============================================================ */

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";

const SPOT_API = "https://data-api.binance.vision";

const DEPTH_WS =
  "wss://fstream.binance.com/ws/btcusdt@depth@100ms";

const KLINE_WS =
  "wss://fstream.binance.com/stream?streams=" +
  "btcusdt@kline_15m/" +
  "btcusdt@kline_1h/" +
  "btcusdt@kline_4h/" +
  "btcusdt@kline_1d";

const FUTURES_API_WS =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* ============================================================
   SETTINGS
   ============================================================ */

const SETTINGS = {

  /* ----------------------------------------------------------
     RAW ORDER BOOK
     ---------------------------------------------------------- */

  rawMinUsd: 50000,

  restingRadiusUsd: 35,
  restingConfirmUsd: 250000,

  /* ----------------------------------------------------------
     STRUCTURE
     ---------------------------------------------------------- */

  mergeUsd: 35,
  zonePct: 0.0015,
  reactionPct: 0.003,

  minScore: 40,
  maxLevels: 12,

  /* ----------------------------------------------------------
     ORDER BOOK CONCENTRATION
     ---------------------------------------------------------- */

  bookBucketUsd: 5,

  orderBucketMinUsd: 250000,

  orderConcentrationMultiple: 2.5,

  orderMinUsd: 750000,

  orderSingleBucketMinUsd: 1000000,

  minStrongBuckets: 2,

  /*
     HARD WIDTH LIMIT.

     A cluster cannot become a huge 120-200 USD
     ladder simply because many neighboring buckets
     are individually large.
  */
  orderMaxClusterWidthUsd: 50,

  /*
     If concentration drops sharply between adjacent
     strong buckets, split the cluster.
  */
  orderSplitRatio: 0.45,

  /*
     Do not allow more than this many strong buckets
     in a single wall cluster.
  */
  orderMaxStrongBucketsPerCluster: 6,

  /*
     A single giant wall can stand by itself.
  */
  orderGiantWallUsd: 1500000,

  /* ----------------------------------------------------------
     PERSISTENCE
     ---------------------------------------------------------- */

  persistencePollMs: 5000,

  persistenceConfirmMs: 60000,

  persistenceMaxAgeMs: 10 * 60 * 1000,

  persistenceTolerancePct: 0.0005,

  persistenceRemovedMinObservations: 2,

  persistenceRemovedMinLifetimeMs: 10000,

  /* ----------------------------------------------------------
     EVENTS
     ---------------------------------------------------------- */

  eventMax: 100,

  /* ----------------------------------------------------------
     SWEEPS
     ---------------------------------------------------------- */

  sweepLookbackMs: 24 * 60 * 60 * 1000,

  sweepMinPenetrationPct: 0.00015,

  /* ----------------------------------------------------------
     MAJOR STRUCTURE
     ---------------------------------------------------------- */

  majorMinDistancePct: 0.50,

  majorMinimumTimeframeWeight: 2,

  major1hMinScore: 90,

  majorHigherTfMinScore: 40,

  /* ----------------------------------------------------------
     STRUCTURE CACHE
     ---------------------------------------------------------- */

  structureCacheMaxAgeMs: 30 * 60 * 1000,

  /* ----------------------------------------------------------
     CONFLUENCE
     ---------------------------------------------------------- */

  confluenceMaxDistancePct: 0.001,

  confluenceMaxDistanceUsd: 75,

  confluenceMinOverlapRatio: 0.20,

  /* ----------------------------------------------------------
     DEPTH SYNC
     ---------------------------------------------------------- */

  snapshotCooldownMs: 5000,

  maxPendingDepthEvents: 5000,

  depthReconnectMs: 3000,

  snapshotReconnectMs: 3000,

  klineReconnectMs: 3000,

  /* ----------------------------------------------------------
     REST
     ---------------------------------------------------------- */

  requestTimeoutMs: 15000
};


/* ============================================================
   STRUCTURE TIMEFRAMES
   ============================================================ */

const STRUCTURE = {

  "15m": {
    interval: "15m",
    weight: 1,
    candles: 300,
    left: 3,
    right: 3,
    eq: 0.0015
  },

  "1h": {
    interval: "1h",
    weight: 2,
    candles: 300,
    left: 4,
    right: 4,
    eq: 0.0015
  },

  "4h": {
    interval: "4h",
    weight: 3,
    candles: 250,
    left: 5,
    right: 5,
    eq: 0.0020
  },

  "1d": {
    interval: "1d",
    weight: 4,
    candles: 180,
    left: 5,
    right: 5,
    eq: 0.0025
  }
};


/* ============================================================
   GLOBAL STATE
   ============================================================ */

const state = {

  symbol: SYMBOL,

  currentPrice: null,

  priceSource: null,

  /* ----------------------------------------------------------
     ORDER BOOK
     ---------------------------------------------------------- */

  bids: new Map(),

  asks: new Map(),

  initialized: false,

  lastUpdateId: 0,

  pendingDepthEvents: [],

  waitingForBridge: false,

  snapshotPending: false,

  snapshotInFlight: false,

  lastSnapshotRequestAt: 0,

  /* ----------------------------------------------------------
     CONNECTIONS
     ---------------------------------------------------------- */

  depthWs: null,

  depthConnected: false,

  snapshotWs: null,

  snapshotConnected: false,

  klineWs: null,

  klineConnected: false,

  /* ----------------------------------------------------------
     SNAPSHOT STATS
     ---------------------------------------------------------- */

  snapshotRequests: 0,

  snapshot429s: 0,

  bridgeAttempts: 0,

  bridgeFound: 0,

  resyncs: 0,

  sequenceGaps: 0,

  /* ----------------------------------------------------------
     CANDLES
     ---------------------------------------------------------- */

  candles: {
    "15m": [],
    "1h": [],
    "4h": [],
    "1d": []
  },

  /* ----------------------------------------------------------
     STRUCTURE
     ---------------------------------------------------------- */

  structure: {

    swingHighs: [],
    swingLows: [],

    levels: [],
    majorLevels: [],
    nearLevels: [],

    lastGoodRebuildAt: null,
    lastAttemptAt: null,

    rebuildCount: 0,
    rejectedRebuilds: 0,

    status: "INITIALIZING",

    lastRebuildAccepted: false,

    lastRebuildReason: null,

    lastCandidateLevels: 0,

    lastCandidateMajor: 0,

    retainedPrevious: false,

    lastGoodLevels: [],
    lastGoodMajorLevels: [],

    lastGoodAt: null
  },

  /* ----------------------------------------------------------
     PERSISTENCE
     ---------------------------------------------------------- */

  persistence: {
    bids: [],
    asks: [],
    removed: []
  },

  /* ----------------------------------------------------------
     EVENTS
     ---------------------------------------------------------- */

  liquidityEvents: [],

  recentSweeps: [],

  /* ----------------------------------------------------------
     ERRORS
     ---------------------------------------------------------- */

  lastError: null,

  updatedAt: null
};


/* ============================================================
   BASIC HELPERS
   ============================================================ */

function now() {
  return Date.now();
}


function round(value, decimals = 2) {

  if (!Number.isFinite(value)) {
    return null;
  }

  const factor = Math.pow(10, decimals);

  return Math.round(value * factor) / factor;
}


function clamp(value, min, max) {

  return Math.max(min, Math.min(max, value));
}


function absPct(a, b) {

  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) {
    return Infinity;
  }

  return Math.abs(a - b) / Math.abs(b);
}


function distancePct(price, currentPrice) {

  if (!Number.isFinite(price) || !Number.isFinite(currentPrice) || currentPrice === 0) {
    return 0;
  }

  return ((price - currentPrice) / currentPrice) * 100;
}


function weightedMid(items) {

  if (!items || !items.length) {
    return null;
  }

  let usd = 0;
  let value = 0;

  for (const item of items) {

    const amount = Number(item.usd) || 0;
    const price = Number(item.price) || 0;

    usd += amount;
    value += price * amount;
  }

  if (!usd) {
    return null;
  }

  return value / usd;
}


function median(values) {

  const arr = values
    .filter(Number.isFinite)
    .slice()
    .sort((a, b) => a - b);

  if (!arr.length) {
    return 0;
  }

  const middle = Math.floor(arr.length / 2);

  if (arr.length % 2) {
    return arr[middle];
  }

  return (arr[middle - 1] + arr[middle]) / 2;
}


/* ============================================================
   PRICE
   ============================================================ */

function updateCurrentPrice() {

  const bids = [...state.bids.keys()]
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => b - a);

  const asks = [...state.asks.keys()]
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);

  if (bids.length && asks.length) {

    state.currentPrice = (bids[0] + asks[0]) / 2;

    state.priceSource = "futures_orderbook_mid";

    return state.currentPrice;
  }

  if (state.currentPrice) {
    return state.currentPrice;
  }

  return null;
}


/* ============================================================
   HTTP FETCH WITH TIMEOUT
   ============================================================ */

async function fetchJson(url) {

  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    SETTINGS.requestTimeoutMs
  );

  try {

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": "BTCUSDT-Liquidity-Relay/10.0"
      },
      signal: controller.signal
    });

    if (!response.ok) {

      const text = await response.text();

      throw new Error(
        `HTTP ${response.status}: ${text.slice(0, 300)}`
      );
    }

    return await response.json();

  } finally {

    clearTimeout(timer);
  }
}


/* ============================================================
   HISTORICAL SPOT CANDLES
   ============================================================ */

async function loadHistoricalCandles(tf) {

  const config = STRUCTURE[tf];

  if (!config) {
    return;
  }

  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${config.interval}` +
    `&limit=${config.candles}`;

  try {

    const data = await fetchJson(url);

    const candles = data
      .map(row => ({

        time: Number(row[0]),

        open: Number(row[1]),

        high: Number(row[2]),

        low: Number(row[3]),

        close: Number(row[4]),

        volume: Number(row[5]),

        closeTime: Number(row[6])

      }))
      .filter(c =>
        Number.isFinite(c.high) &&
        Number.isFinite(c.low) &&
        Number.isFinite(c.close)
      );

    state.candles[tf] = candles;

    console.log(
      `Historical ${tf}: ${candles.length} candles`
    );

  } catch (error) {

    state.lastError = `Historical ${tf}: ${error.message}`;

    console.error(
      `Historical ${tf} failed:`,
      error.message
    );
  }
}


async function loadAllHistoricalCandles() {

  await Promise.all(
    Object.keys(STRUCTURE).map(loadHistoricalCandles)
  );

  rebuildStructure();
}


/* ============================================================
   PIVOT DETECTION
   ============================================================ */

function detectPivots(candles, left, right) {

  const highs = [];
  const lows = [];

  if (!candles || candles.length < left + right + 5) {
    return {
      highs,
      lows
    };
  }

  for (
    let i = left;
    i < candles.length - right;
    i++
  ) {

    const candle = candles[i];

    let isHigh = true;
    let isLow = true;

    for (
      let j = i - left;
      j <= i + right;
      j++
    ) {

      if (j === i) {
        continue;
      }

      if (candles[j].high >= candle.high) {
        isHigh = false;
      }

      if (candles[j].low <= candle.low) {
        isLow = false;
      }

      if (!isHigh && !isLow) {
        break;
      }
    }

    if (isHigh) {

      highs.push({
        price: candle.high,
        time: candle.time,
        index: i
      });
    }

    if (isLow) {

      lows.push({
        price: candle.low,
        time: candle.time,
        index: i
      });
    }
  }

  return {
    highs,
    lows
  };
}


/* ============================================================
   CLUSTER STRUCTURAL PIVOTS
   ============================================================ */

function clusterPivots(pivots, tolerancePct) {

  const clusters = [];

  const sorted = pivots
    .slice()
    .sort((a, b) => a.price - b.price);

  for (const pivot of sorted) {

    let best = null;
    let bestDistance = Infinity;

    for (const cluster of clusters) {

      const distance = absPct(
        pivot.price,
        cluster.price
      );

      if (
        distance <= tolerancePct &&
        distance < bestDistance
      ) {

        best = cluster;
        bestDistance = distance;
      }
    }

    if (!best) {

      clusters.push({

        price: pivot.price,

        priceLow: pivot.price,

        priceHigh: pivot.price,

        members: [pivot],

        times: [pivot.time]
      });

    } else {

      best.members.push(pivot);

      best.times.push(pivot.time);

      best.priceLow = Math.min(
        best.priceLow,
        pivot.price
      );

      best.priceHigh = Math.max(
        best.priceHigh,
        pivot.price
      );

      best.price =
        best.members.reduce(
          (sum, item) => sum + item.price,
          0
        ) / best.members.length;
    }
  }

  return clusters;
}


/* ============================================================
   REACTION COUNT
   ============================================================ */

function countReactions(candles, cluster, side) {

  let reactions = 0;

  if (!candles || !candles.length) {
    return reactions;
  }

  const zoneLow =
    cluster.priceLow *
    (1 - SETTINGS.reactionPct);

  const zoneHigh =
    cluster.priceHigh *
    (1 + SETTINGS.reactionPct);

  for (let i = 0; i < candles.length; i++) {

    const candle = candles[i];

    let touched = false;

    if (side === "BSL") {

      touched =
        candle.high >= zoneLow &&
        candle.high <= zoneHigh;

    } else {

      touched =
        candle.low >= zoneLow &&
        candle.low <= zoneHigh;
    }

    if (!touched) {
      continue;
    }

    const end =
      Math.min(
        candles.length - 1,
        i + 8
      );

    for (let j = i + 1; j <= end; j++) {

      const future = candles[j];

      if (side === "BSL") {

        if (
          future.low <=
          cluster.price *
          (1 - SETTINGS.reactionPct)
        ) {

          reactions++;
          break;
        }

      } else {

        if (
          future.high >=
          cluster.price *
          (1 + SETTINGS.reactionPct)
        ) {

          reactions++;
          break;
        }
      }
    }
  }

  return reactions;
}


/* ============================================================
   BUILD TIMEFRAME STRUCTURE
   ============================================================ */

function buildTFStructure(tf) {

  const config = STRUCTURE[tf];

  const candles = state.candles[tf];

  if (!config || !candles || candles.length < 20) {

    return {
      highs: [],
      lows: []
    };
  }

  const pivots = detectPivots(
    candles,
    config.left,
    config.right
  );

  const highClusters =
    clusterPivots(
      pivots.highs,
      config.eq
    );

  const lowClusters =
    clusterPivots(
      pivots.lows,
      config.eq
    );

  const highs = highClusters.map(cluster => ({

    side: "BSL",

    price: cluster.price,

    priceLow: cluster.priceLow,

    priceHigh: cluster.priceHigh,

    equalCount: cluster.members.length,

    reactions:
      countReactions(
        candles,
        cluster,
        "BSL"
      ),

    timeframe: tf,

    timeframeWeight: config.weight,

    lastTime:
      Math.max(...cluster.times)
  }));

  const lows = lowClusters.map(cluster => ({

    side: "SSL",

    price: cluster.price,

    priceLow: cluster.priceLow,

    priceHigh: cluster.priceHigh,

    equalCount: cluster.members.length,

    reactions:
      countReactions(
        candles,
        cluster,
        "SSL"
      ),

    timeframe: tf,

    timeframeWeight: config.weight,

    lastTime:
      Math.max(...cluster.times)
  }));

  return {
    highs,
    lows
  };
}


/* ============================================================
   MERGE STRUCTURE ACROSS TIMEFRAMES
   ============================================================ */

function mergeStructureLevels(levels) {

  const merged = [];

  const sorted = levels
    .slice()
    .sort((a, b) => a.price - b.price);

  for (const level of sorted) {

    let best = null;
    let bestDistance = Infinity;

    for (const existing of merged) {

      if (existing.side !== level.side) {
        continue;
      }

      const distance =
        absPct(
          level.price,
          existing.price
        );

      const tolerance =
        Math.max(
          SETTINGS.zonePct,
          SETTINGS.mergeUsd /
          Math.max(level.price, 1)
        );

      if (
        distance <= tolerance &&
        distance < bestDistance
      ) {

        best = existing;
        bestDistance = distance;
      }
    }

    if (!best) {

      merged.push({

        side: level.side,

        price: level.price,

        priceLow: level.priceLow,

        priceHigh: level.priceHigh,

        equalCount: level.equalCount,

        reactions: level.reactions,

        timeframes: [
          level.timeframe
        ],

        timeframeWeight:
          level.timeframeWeight,

        scoreBase: 0,

        score: 0,

        restingUsd: 0,

        restingConfirmed: false,

        restingLevels: []
      });

    } else {

      best.price =
        (
          best.price +
          level.price
        ) / 2;

      best.priceLow =
        Math.min(
          best.priceLow,
          level.priceLow
        );

      best.priceHigh =
        Math.max(
          best.priceHigh,
          level.priceHigh
        );

      best.equalCount +=
        level.equalCount;

      best.reactions +=
        level.reactions;

      if (
        !best.timeframes.includes(
          level.timeframe
        )
      ) {

        best.timeframes.push(
          level.timeframe
        );
      }

      best.timeframeWeight +=
        level.timeframeWeight;
    }
  }

  return merged;
}


/* ============================================================
   STRUCTURAL SCORE
   ============================================================ */

function scoreStructureLevel(level) {

  const timeframeScore =
    level.timeframeWeight * 10;

  const equalScore =
    Math.min(
      level.equalCount,
      30
    ) * 5;

  const reactionScore =
    Math.min(
      level.reactions,
      40
    ) * 2;

  const score =
    timeframeScore +
    equalScore +
    reactionScore;

  level.scoreBase = score;

  level.score = score;

  if (
    score >= 125 ||
    (
      level.timeframes.length >= 3 &&
      level.equalCount >= 4
    )
  ) {

    level.strength = "HIGH";

  } else if (
    score >= 90 ||
    level.timeframes.length >= 2 ||
    level.equalCount >= 3
  ) {

    level.strength = "MEDIUM";

  } else {

    level.strength = "LOW";
  }

  return level;
}


/* ============================================================
   PRICE DIRECTION FILTER
   ============================================================ */

function isDirectionalLevel(level, currentPrice) {

  if (!Number.isFinite(currentPrice)) {
    return false;
  }

  /*
     BSL MUST BE ABOVE PRICE.
  */

  if (
    level.side === "BSL" &&
    level.price <= currentPrice
  ) {

    return false;
  }

  /*
     SSL MUST BE BELOW PRICE.
  */

  if (
    level.side === "SSL" &&
    level.price >= currentPrice
  ) {

    return false;
  }

  return true;
}


/* ============================================================
   MAJOR LEVEL FILTER
   ============================================================ */

function isMajorLevel(level) {

  const currentPrice =
    state.currentPrice;

  if (!Number.isFinite(currentPrice)) {
    return false;
  }

  if (
    !isDirectionalLevel(
      level,
      currentPrice
    )
  ) {

    return false;
  }

  const distance =
    Math.abs(
      level.price - currentPrice
    ) / currentPrice;

  if (
    distance <
    SETTINGS.majorMinDistancePct / 100
  ) {

    return false;
  }

  if (
    level.timeframeWeight <
    SETTINGS.majorMinimumTimeframeWeight
  ) {

    return false;
  }

  /*
     A lone 1H level needs stronger evidence.
  */

  if (
    level.timeframes.length === 1 &&
    level.timeframes.includes("1h") &&
    level.score < SETTINGS.major1hMinScore
  ) {

    return false;
  }

  /*
     Higher timeframe single levels can qualify
     with a lower score.
  */

  if (
    level.timeframes.length === 1 &&
    (
      level.timeframes.includes("4h") ||
      level.timeframes.includes("1d")
    ) &&
    level.score <
    SETTINGS.majorHigherTfMinScore
  ) {

    return false;
  }

  return true;
}


/* ============================================================
   NEAR LEVEL
   ============================================================ */

function isNearLevel(level) {

  const currentPrice =
    state.currentPrice;

  if (!Number.isFinite(currentPrice)) {
    return false;
  }

  if (
    !isDirectionalLevel(
      level,
      currentPrice
    )
  ) {

    return false;
  }

  const distance =
    Math.abs(
      level.price - currentPrice
    ) / currentPrice;

  return distance < 0.005;
}


/* ============================================================
   RESTING LIQUIDITY NEAR STRUCTURAL LEVEL
   ============================================================ */

function getRestingLiquidity(level) {

  const map =
    level.side === "BSL"
      ? state.asks
      : state.bids;

  const rows = [];

  let usd = 0;

  for (
    const [
      priceString,
      quantity
    ] of map.entries()
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty)
    ) {
      continue;
    }

    const value =
      price * qty;

    if (
      value <
      SETTINGS.rawMinUsd
    ) {
      continue;
    }

    if (
      Math.abs(
        price - level.price
      ) <=
      SETTINGS.restingRadiusUsd
    ) {

      usd += value;

      rows.push({

        price,

        quantity: qty,

        usd: value
      });
    }
  }

  rows.sort(
    (a, b) => b.usd - a.usd
  );

  level.restingUsd = usd;

  level.restingConfirmed =
    usd >= SETTINGS.restingConfirmUsd;

  level.restingLevels =
    rows.slice(0, 100);

  /*
     Resting liquidity adds evidence,
     but does not create structural liquidity.
  */

  level.score =
    level.scoreBase +
    clamp(
      usd / 100000,
      0,
      25
    );

  if (level.score >= 125) {
    level.strength = "HIGH";
  } else if (level.score >= 90) {
    level.strength = "MEDIUM";
  } else {
    level.strength = "LOW";
  }

  return level;
}


/* ============================================================
   REBUILD STRUCTURE
   ============================================================ */

function rebuildStructure() {

  const structure =
    state.structure;

  structure.lastAttemptAt =
    new Date().toISOString();

  structure.rebuildCount++;

  const allHighs = [];
  const allLows = [];

  for (
    const tf of Object.keys(STRUCTURE)
  ) {

    const tfStructure =
      buildTFStructure(tf);

    allHighs.push(
      ...tfStructure.highs
    );

    allLows.push(
      ...tfStructure.lows
    );
  }

  const mergedHighs =
    mergeStructureLevels(
      allHighs
    );

  const mergedLows =
    mergeStructureLevels(
      allLows
    );

  const candidateLevels =
    [
      ...mergedHighs,
      ...mergedLows
    ]
    .map(scoreStructureLevel);

  /*
     VERY IMPORTANT:
     directional filtering happens AFTER
     current price is known.
  */

  const directional =
    candidateLevels.filter(level =>
      isDirectionalLevel(
        level,
        state.currentPrice
      )
    );

  const major =
    directional.filter(
      isMajorLevel
    );

  const near =
    directional.filter(
      isNearLevel
    );

  structure.lastCandidateLevels =
    directional.length;

  structure.lastCandidateMajor =
    major.length;

  structure.lastRebuildAt =
    new Date().toISOString();

  /*
     Reject only genuinely broken candidates.
  */

  if (
    !directional.length
  ) {

    structure.rejectedRebuilds++;

    structure.lastRebuildAccepted =
      false;

    structure.lastRebuildReason =
      "rejected_empty_directional_structure";

    const oldAge =
      structure.lastGoodAt
        ? now() -
          new Date(
            structure.lastGoodAt
          ).getTime()
        : Infinity;

    if (
      structure.lastGoodLevels.length &&
      oldAge <
      SETTINGS.structureCacheMaxAgeMs
    ) {

      structure.levels =
        structure.lastGoodLevels.map(
          x => ({ ...x })
        );

      structure.majorLevels =
        structure.lastGoodMajorLevels.map(
          x => ({ ...x })
        );

      structure.nearLevels =
        structure.levels.filter(
          isNearLevel
        );

      structure.retainedPrevious = true;

      structure.status =
        "STALE_FALLBACK";

      structure.lastRebuildReason =
        "retained_previous_major_structure";

      return;
    }

    structure.levels = [];
    structure.majorLevels = [];
    structure.nearLevels = [];

    structure.retainedPrevious = false;

    structure.status =
      "NO_STRUCTURE";

    return;
  }

  /*
     Candidate accepted.
  */

  structure.levels =
    directional.map(level =>
      getRestingLiquidity(level)
    );

  structure.majorLevels =
    structure.levels
      .filter(isMajorLevel)
      .sort(
        (a, b) =>
          Math.abs(
            a.price - state.currentPrice
          ) -
          Math.abs(
            b.price - state.currentPrice
          )
      );

  structure.nearLevels =
    structure.levels
      .filter(isNearLevel)
      .sort(
        (a, b) =>
          Math.abs(
            a.price - state.currentPrice
          ) -
          Math.abs(
            b.price - state.currentPrice
          )
      );

  structure.lastGoodLevels =
    structure.levels.map(
      x => ({ ...x })
    );

  structure.lastGoodMajorLevels =
    structure.majorLevels.map(
      x => ({ ...x })
    );

  structure.lastGoodAt =
    new Date().toISOString();

  structure.lastGoodRebuildAt =
    structure.lastGoodAt;

  structure.lastRebuildAccepted =
    true;

  structure.retainedPrevious =
    false;

  structure.lastRebuildReason =
    structure.majorLevels.length
      ? "accepted_with_major_structure"
      : "accepted_without_major_structure";

  structure.status =
    structure.majorLevels.length
      ? "HEALTHY"
      : "HEALTHY_NO_MAJOR";
}


/* ============================================================
   STRUCTURE LEVEL HELPERS
   ============================================================ */

function getMajorLevels(side) {

  return state.structure.majorLevels
    .filter(level =>
      level.side === side &&
      isDirectionalLevel(
        level,
        state.currentPrice
      )
    )
    .sort((a, b) => {

      if (side === "BSL") {
        return a.price - b.price;
      }

      return b.price - a.price;
    });
}


function getNearLevels(side) {

  return state.structure.nearLevels
    .filter(level =>
      level.side === side &&
      isDirectionalLevel(
        level,
        state.currentPrice
      )
    )
    .sort(
      (a, b) =>
        Math.abs(
          a.price -
          state.currentPrice
        ) -
        Math.abs(
          b.price -
          state.currentPrice
        )
    );
}


/* ============================================================
   ORDER BOOK ROWS
   ============================================================ */

function getBookRows(side) {

  const map =
    side === "bid"
      ? state.bids
      : state.asks;

  const rows = [];

  for (
    const [
      priceString,
      quantity
    ] of map.entries()
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty) ||
      qty <= 0
    ) {
      continue;
    }

    const usd =
      price * qty;

    if (
      usd <
      SETTINGS.rawMinUsd
    ) {
      continue;
    }

    rows.push({

      price,

      quantity: qty,

      usd
    });
  }

  return rows;
}


/* ============================================================
   ORDER BOOK BUCKETS
   ============================================================ */

function makeBookBuckets(side) {

  const rows =
    getBookRows(side);

  const bucketSize =
    SETTINGS.bookBucketUsd;

  const buckets = new Map();

  for (const row of rows) {

    const bucketPrice =
      Math.floor(
        row.price / bucketSize
      ) * bucketSize;

    const key =
      bucketPrice.toFixed(2);

    let bucket =
      buckets.get(key);

    if (!bucket) {

      bucket = {

        priceLow:
          bucketPrice,

        priceHigh:
          bucketPrice +
          bucketSize,

        center:
          bucketPrice +
          bucketSize / 2,

        usd: 0,

        rawLevels: 0,

        strongestPrice:
          row.price,

        strongestUsd:
          row.usd
      };

      buckets.set(
        key,
        bucket
      );
    }

    bucket.usd += row.usd;

    bucket.rawLevels++;

    if (
      row.usd >
      bucket.strongestUsd
    ) {

      bucket.strongestUsd =
        row.usd;

      bucket.strongestPrice =
        row.price;
    }
  }

  return [...buckets.values()]
    .sort(
      (a, b) =>
        a.priceLow -
        b.priceLow
    );
}


/* ============================================================
   ORDER BOOK CONCENTRATED WALL DETECTOR
   ============================================================ */

function buildBookClusters(side) {

  const buckets =
    makeBookBuckets(side);

  if (!buckets.length) {
    return [];
  }

  const medianBucketUsd =
    median(
      buckets.map(
        b => b.usd
      )
    );

  const concentrationThreshold =
    Math.max(
      SETTINGS.orderBucketMinUsd,
      medianBucketUsd *
      SETTINGS.orderConcentrationMultiple
    );

  /*
     Strong bucket means genuinely concentrated
     compared with the current side of the book.
  */

  const marked =
    buckets.map(bucket => ({

      ...bucket,

      strong:
        bucket.usd >=
        concentrationThreshold,

      giant:
        bucket.usd >=
        SETTINGS.orderGiantWallUsd
    }));

  const clusters = [];

  let current = [];

  function flush() {

    if (!current.length) {
      return;
    }

    const strongBuckets =
      current.filter(
        b => b.strong
      );

    if (!strongBuckets.length) {

      current = [];

      return;
    }

    /*
       A cluster is accepted when:
       - it has enough strong buckets, OR
       - it contains one genuine giant wall.
    */

    const totalUsd =
      strongBuckets.reduce(
        (sum, b) =>
          sum + b.usd,
        0
      );

    const hasGiant =
      strongBuckets.some(
        b => b.giant
      );

    if (
      strongBuckets.length <
      SETTINGS.minStrongBuckets &&
      !hasGiant
    ) {

      current = [];

      return;
    }

    if (
      totalUsd <
      SETTINGS.orderMinUsd &&
      !hasGiant
    ) {

      current = [];

      return;
    }

    /*
       Only strong buckets contribute to cluster USD.
       This is the important fix that prevents a
       normal dense ladder from becoming a $30M wall.
    */

    const first =
      strongBuckets[0];

    const last =
      strongBuckets[
        strongBuckets.length - 1
      ];

    const priceLow =
      first.priceLow;

    const priceHigh =
      last.priceHigh;

    const width =
      priceHigh -
      priceLow;

    if (
      width >
      SETTINGS.orderMaxClusterWidthUsd
    ) {

      /*
         Split oversized runs into smaller
         independent clusters.
      */

      let segment = [];

      for (
        const bucket of strongBuckets
      ) {

        if (!segment.length) {

          segment = [bucket];

          continue;
        }

        const segmentLow =
          segment[0].priceLow;

        const candidateWidth =
          bucket.priceHigh -
          segmentLow;

        if (
          candidateWidth <=
          SETTINGS.orderMaxClusterWidthUsd &&
          segment.length <
          SETTINGS.orderMaxStrongBucketsPerCluster
        ) {

          segment.push(bucket);

        } else {

          addStrongSegment(
            segment,
            medianBucketUsd,
            concentrationThreshold,
            side,
            clusters
          );

          segment = [bucket];
        }
      }

      if (segment.length) {

        addStrongSegment(
          segment,
          medianBucketUsd,
          concentrationThreshold,
          side,
          clusters
        );
      }

    } else {

      addStrongSegment(
        strongBuckets,
        medianBucketUsd,
        concentrationThreshold,
        side,
        clusters
      );
    }

    current = [];
  }


  function processBucket(bucket) {

    if (!bucket.strong) {

      /*
         Weak bucket creates a concentration valley.
         It therefore breaks the cluster.
      */

      flush();

      return;
    }

    if (!current.length) {

      current = [bucket];

      return;
    }

    const previous =
      current[
        current.length - 1
      ];

    const gap =
      bucket.priceLow -
      previous.priceHigh;

    const currentWidth =
      bucket.priceHigh -
      current[0].priceLow;

    /*
       Hard physical gap.
    */

    if (
      gap >
      SETTINGS.bookBucketUsd * 1.5
    ) {

      flush();

      current = [bucket];

      return;
    }

    /*
       Width limit.
    */

    if (
      currentWidth >
      SETTINGS.orderMaxClusterWidthUsd
    ) {

      flush();

      current = [bucket];

      return;
    }

    /*
       Concentration valley.

       If current bucket is much weaker than the
       previous strong bucket, split it.
    */

    if (
      bucket.usd <
      previous.usd *
      SETTINGS.orderSplitRatio
    ) {

      flush();

      current = [bucket];

      return;
    }

    if (
      current.length >=
      SETTINGS.orderMaxStrongBucketsPerCluster
    ) {

      flush();

      current = [bucket];

      return;
    }

    current.push(bucket);
  }


  for (
    const bucket of marked
  ) {

    processBucket(bucket);
  }

  flush();

  return clusters
    .sort(
      (a, b) =>
        b.usd - a.usd
    )
    .slice(0, 20);
}


/* ============================================================
   ADD STRONG BOOK SEGMENT
   ============================================================ */

function addStrongSegment(
  segment,
  medianBucketUsd,
  concentrationThreshold,
  side,
  output
) {

  if (!segment.length) {
    return;
  }

  const usd =
    segment.reduce(
      (sum, bucket) =>
        sum + bucket.usd,
      0
    );

  const giant =
    segment.some(
      bucket =>
        bucket.usd >=
        SETTINGS.orderGiantWallUsd
    );

  if (
    segment.length <
    SETTINGS.minStrongBuckets &&
    !giant
  ) {

    if (
      !segment.some(
        bucket =>
          bucket.usd >=
          SETTINGS.orderSingleBucketMinUsd
      )
    ) {

      return;
    }
  }

  if (
    usd <
    SETTINGS.orderMinUsd &&
    !giant
  ) {

    return;
  }

  const priceLow =
    segment[0].priceLow;

  const priceHigh =
    segment[
      segment.length - 1
    ].priceHigh;

  const width =
    priceHigh - priceLow;

  if (
    width >
    SETTINGS.orderMaxClusterWidthUsd
  ) {

    return;
  }

  const weighted =
    weightedMid(
      segment.map(
        bucket => ({
          price:
            bucket.center,

          usd:
            bucket.usd
        })
      )
    );

  let strongest =
    segment[0];

  for (
    const bucket of segment
  ) {

    if (
      bucket.usd >
      strongest.usd
    ) {

      strongest =
        bucket;
    }
  }

  /*
     Concentration ratio:

     cluster USD divided by all book liquidity
     represented by the same bucket span.

     Higher = more concentrated.
  */

  const concentrationRatio =
    usd /
    Math.max(
      usd,
      medianBucketUsd *
      Math.max(
        segment.length,
        1
      )
    );

  output.push({

    side,

    priceLow,

    priceHigh,

    midpoint:
      weighted ||
      (
        priceLow +
        priceHigh
      ) / 2,

    usd,

    levels:
      segment.length,

    rawLevels:
      segment.reduce(
        (sum, b) =>
          sum + b.rawLevels,
        0
      ),

    strongBuckets:
      segment.length,

    strongestPrice:
      strongest.strongestPrice,

    strongestBucketUsd:
      strongest.usd,

    strongestRawOrderUsd:
      strongest.strongestUsd,

    medianBucketUsd,

    concentrationThreshold,

    concentrationRatio,

    width,

    giantWall:
      giant
  });
}


/* ============================================================
   STRUCTURAL CONFLUENCE
   ============================================================ */

function findStructuralConfluence(cluster) {

  const major =
    state.structure.majorLevels
      .filter(level =>
        isMajorLevel(level)
      );

  let best = null;

  for (const level of major) {

    if (
      level.side === "BSL" &&
      cluster.side !== "ask"
    ) {
      continue;
    }

    if (
      level.side === "SSL" &&
      cluster.side !== "bid"
    ) {
      continue;
    }

    const overlapLow =
      Math.max(
        cluster.priceLow,
        level.priceLow
      );

    const overlapHigh =
      Math.min(
        cluster.priceHigh,
        level.priceHigh
      );

    const overlap =
      Math.max(
        0,
        overlapHigh -
        overlapLow
      );

    const clusterWidth =
      Math.max(
        1,
        cluster.priceHigh -
        cluster.priceLow
      );

    const overlapRatio =
      overlap /
      clusterWidth;

    const strongestInside =
      cluster.strongestPrice >=
        level.priceLow &&
      cluster.strongestPrice <=
        level.priceHigh;

    const distance =
      Math.abs(
        cluster.midpoint -
        level.price
      );

    const allowedDistance =
      Math.max(
        SETTINGS.confluenceMaxDistanceUsd,
        level.price *
        SETTINGS.confluenceMaxDistancePct
      );

    const qualified =
      strongestInside ||
      overlapRatio >=
        SETTINGS.confluenceMinOverlapRatio ||
      distance <=
        allowedDistance;

    if (!qualified) {
      continue;
    }

    const quality =
      (
        strongestInside
          ? 100
          : 0
      ) +
      overlapRatio * 50 +
      Math.max(
        0,
        1 -
        distance /
        Math.max(
          allowedDistance,
          1
        )
      ) * 25 +
      level.score / 20;

    if (
      !best ||
      quality > best.quality
    ) {

      best = {

        structuralConfluence:
          true,

        structuralLevel:
          level.price,

        structuralZoneLow:
          level.priceLow,

        structuralZoneHigh:
          level.priceHigh,

        structuralTimeframes:
          level.timeframes,

        structuralStrength:
          level.strength,

        structuralScore:
          level.score,

        structuralOverlap:
          overlap > 0,

        structuralOverlapRatio:
          overlapRatio,

        structuralDistance:
          distance,

        quality
      };
    }
  }

  if (!best) {

    return {

      structuralConfluence: false,

      structuralLevel: null,

      structuralZoneLow: null,

      structuralZoneHigh: null,

      structuralTimeframes: [],

      structuralStrength: null,

      structuralScore: null,

      structuralOverlap: false,

      structuralOverlapRatio: 0,

      structuralDistance: null
    };
  }

  return best;
}


/* ============================================================
   ORDER BOOK OUTPUT
   ============================================================ */

function buildOrderBookIntelligence() {

  const bids =
    buildBookClusters("bid");

  const asks =
    buildBookClusters("ask");

  for (const cluster of bids) {

    Object.assign(
      cluster,
      findStructuralConfluence(
        cluster
      )
    );
  }

  for (const cluster of asks) {

    Object.assign(
      cluster,
      findStructuralConfluence(
        cluster
      )
    );
  }

  /*
     Only return the most meaningful walls.

     Sorting by concentration + USD rather than
     simply USD prevents a giant ladder from
     dominating the output.
  */

  const sortWalls =
    list =>
      list
        .sort((a, b) => {

          const scoreA =
            a.usd *
            (
              1 +
              Math.min(
                a.concentrationRatio,
                3
              ) * 0.25
            );

          const scoreB =
            b.usd *
            (
              1 +
              Math.min(
                b.concentrationRatio,
                3
              ) * 0.25
            );

          return scoreB - scoreA;
        })
        .slice(0, 8);

  return {

    bids:
      sortWalls(bids),

    asks:
      sortWalls(asks)
  };
}


/* ============================================================
   PERSISTENCE MATCHING
   ============================================================ */

function clustersMatch(a, b) {

  if (
    !a ||
    !b ||
    a.side !== b.side
  ) {

    return false;
  }

  const overlapLow =
    Math.max(
      a.priceLow,
      b.priceLow
    );

  const overlapHigh =
    Math.min(
      a.priceHigh,
      b.priceHigh
    );

  if (
    overlapHigh >=
    overlapLow
  ) {

    return true;
  }

  const tolerance =
    Math.max(
      20,
      (
        state.currentPrice ||
        0
      ) *
      SETTINGS.persistenceTolerancePct
    );

  return (
    Math.abs(
      a.midpoint -
      b.midpoint
    ) <= tolerance
  );
}


/* ============================================================
   EVENT
   ============================================================ */

function addLiquidityEvent(event) {

  state.liquidityEvents.unshift({

    ...event,

    eventTime:
      new Date().toISOString()
  });

  if (
    state.liquidityEvents.length >
    SETTINGS.eventMax
  ) {

    state.liquidityEvents =
      state.liquidityEvents.slice(
        0,
        SETTINGS.eventMax
      );
  }
}


/* ============================================================
   PERSISTENCE UPDATE
   ============================================================ */

function updatePersistence() {

  const orderBook =
    buildOrderBookIntelligence();

  const timestamp =
    now();

  for (
    const side of ["bid", "ask"]
  ) {

    const current =
      side === "bid"
        ? orderBook.bids
        : orderBook.asks;

    const previous =
      state.persistence[
        side === "bid"
          ? "bids"
          : "asks"
      ];

    const matched =
      new Set();

    const next = [];

    for (
      const cluster of current
    ) {

      let matchIndex = -1;

      for (
        let i = 0;
        i < previous.length;
        i++
      ) {

        if (
          matched.has(i)
        ) {
          continue;
        }

        if (
          clustersMatch(
            previous[i],
            cluster
          )
        ) {

          matchIndex = i;

          break;
        }
      }

      if (
        matchIndex >= 0
      ) {

        matched.add(
          matchIndex
        );

        const old =
          previous[matchIndex];

        const previousUsd =
          Number(
            old.usd
          ) || 0;

        const currentUsd =
          Number(
            cluster.usd
          ) || 0;

        const firstSeen =
          old.firstSeen ||
          timestamp;

        const durationMs =
          timestamp -
          firstSeen;

        const observations =
          (
            old.observations ||
            0
          ) + 1;

        const updated = {

          ...cluster,

          firstSeen,

          lastSeen:
            timestamp,

          durationMs,

          durationSec:
            Math.floor(
              durationMs / 1000
            ),

          observations,

          maxUsd:
            Math.max(
              old.maxUsd || 0,
              currentUsd
            ),

          previousUsd,

          changeUsd:
            currentUsd -
            previousUsd,

          status:
            durationMs >=
            SETTINGS.persistenceConfirmMs
              ? "HOLDING"
              : "BUILDING",

          persistent:
            durationMs >=
            SETTINGS.persistenceConfirmMs,

          persistentEventSent:
            !!old.persistentEventSent,

          confluenceEventSent:
            !!old.confluenceEventSent,

          strengthEventAt:
            old.strengthEventAt || 0,

          removedEventSent:
            !!old.removedEventSent,

          removed: false
        };

        const confluence =
          findStructuralConfluence(
            updated
          );

        Object.assign(
          updated,
          confluence
        );

        /*
           First persistent event.
        */

        if (
          updated.persistent &&
          !updated.persistentEventSent
        ) {

          addLiquidityEvent({

            type: "PERSISTENT",

            side:
              updated.side,

            midpoint:
              updated.midpoint,

            priceLow:
              updated.priceLow,

            priceHigh:
              updated.priceHigh,

            usd:
              updated.usd,

            durationSec:
              updated.durationSec,

            strongestPrice:
              updated.strongestPrice,

            strongestBucketUsd:
              updated.strongestBucketUsd
          });

          updated.persistentEventSent =
            true;
        }

        /*
           First structural confluence.
        */

        if (
          updated.persistent &&
          updated.structuralConfluence &&
          !updated.confluenceEventSent
        ) {

          addLiquidityEvent({

            type: "CONFLUENCE",

            side:
              updated.side,

            midpoint:
              updated.midpoint,

            usd:
              updated.usd,

            structuralLevel:
              updated.structuralLevel,

            structuralZoneLow:
              updated.structuralZoneLow,

            structuralZoneHigh:
              updated.structuralZoneHigh,

            structuralTimeframes:
              updated.structuralTimeframes
          });

          updated.confluenceEventSent =
            true;
        }

        /*
           Meaningful strengthening / weakening.

           Do not spam events every 5 seconds.
        */

        const changePct =
          previousUsd > 0
            ? (
                Math.abs(
                  currentUsd -
                  previousUsd
                ) /
                previousUsd
              )
            : 0;

        if (
          updated.persistent &&
          changePct >= 0.10 &&
          timestamp -
            updated.strengthEventAt >=
            15000
        ) {

          addLiquidityEvent({

            type:
              currentUsd >
              previousUsd
                ? "STRENGTHENING"
                : "WEAKENING",

            side:
              updated.side,

            midpoint:
              updated.midpoint,

            usd:
              updated.usd,

            previousUsd,

            changeUsd:
              currentUsd -
              previousUsd,

            changePct:
              changePct * 100
          });

          updated.strengthEventAt =
            timestamp;
        }

        next.push(updated);

      } else {

        const confluence =
          findStructuralConfluence(
            cluster
          );

        next.push({

          ...cluster,

          ...confluence,

          firstSeen:
            timestamp,

          lastSeen:
            timestamp,

          durationMs: 0,

          durationSec: 0,

          observations: 1,

          maxUsd:
            cluster.usd,

          previousUsd: 0,

          changeUsd:
            cluster.usd,

          status: "NEW",

          persistent: false,

          persistentEventSent: false,

          confluenceEventSent: false,

          strengthEventAt: 0,

          removedEventSent: false,

          removed: false
        });
      }
    }

    /*
       Detect meaningful removals.
    */

    for (
      let i = 0;
      i < previous.length;
      i++
    ) {

      if (
        matched.has(i)
      ) {
        continue;
      }

      const old =
        previous[i];

      const lifetime =
        timestamp -
        (
          old.firstSeen ||
          timestamp
        );

      const observations =
        old.observations || 0;

      const meaningfulRemoval =
        observations >=
          SETTINGS.persistenceRemovedMinObservations ||
        lifetime >=
          SETTINGS.persistenceRemovedMinLifetimeMs;

      if (
        meaningfulRemoval &&
        !old.removedEventSent
      ) {

        addLiquidityEvent({

          type: "REMOVED",

          side:
            old.side,

          midpoint:
            old.midpoint,

          priceLow:
            old.priceLow,

          priceHigh:
            old.priceHigh,

          usd:
            old.usd,

          lifetimeSec:
            Math.floor(
              lifetime / 1000
            ),

          observations,

          persistent:
            !!old.persistent
        });

        old.removedEventSent =
          true;
      }

      /*
         Keep removed entries briefly for diagnostics,
         but never keep one-observation noise.
      */

      if (
        meaningfulRemoval
      ) {

        state.persistence.removed.push({

          ...old,

          removed: true,

          removedAt:
            timestamp,

          lifetimeMs:
            lifetime,

          lifetimeSec:
            Math.floor(
              lifetime / 1000
            )
        });
      }
    }

    state.persistence[
      side === "bid"
        ? "bids"
        : "asks"
    ] = next;
  }

  /*
     Cleanup removed records.
  */

  state.persistence.removed =
    state.persistence.removed
      .filter(item =>
        timestamp -
        (
          item.removedAt ||
          timestamp
        ) <
        SETTINGS.persistenceMaxAgeMs
      );

  state.updatedAt =
    new Date().toISOString();
}


/* ============================================================
   TRUE LIQUIDITY SWEEPS
   ============================================================ */

function detectCandleSweeps(
  tf,
  candle
) {

  if (
    !candle ||
    !Number.isFinite(
      candle.high
    ) ||
    !Number.isFinite(
      candle.low
    )
  ) {

    return;
  }

  const candleAge =
    now() -
    candle.closeTime;

  if (
    candleAge >
    SETTINGS.sweepLookbackMs
  ) {

    return;
  }

  /*
     IMPORTANT:
     use structure existing BEFORE the rebuild.
  */

  const majorLevels =
    state.structure.majorLevels
      .slice();

  for (
    const level of majorLevels
  ) {

    /*
       BUY-SIDE LIQUIDITY SWEEP

       Price trades above the complete
       structural zone and closes back below it.
    */

    if (
      level.side === "BSL"
    ) {

      const penetration =
        level.price *
        SETTINGS.sweepMinPenetrationPct;

      const swept =
        candle.high >=
        level.priceHigh +
        penetration;

      const reclaimed =
        candle.close <
        level.priceHigh;

      if (
        swept &&
        reclaimed
      ) {

        registerSweep({

          side: "BSL",

          level,

          tf,

          candle,

          sweepPrice:
            candle.high
        });
      }
    }

    /*
       SELL-SIDE LIQUIDITY SWEEP
    */

    if (
      level.side === "SSL"
    ) {

      const penetration =
        level.price *
        SETTINGS.sweepMinPenetrationPct;

      const swept =
        candle.low <=
        level.priceLow -
        penetration;

      const reclaimed =
        candle.close >
        level.priceLow;

      if (
        swept &&
        reclaimed
      ) {

        registerSweep({

          side: "SSL",

          level,

          tf,

          candle,

          sweepPrice:
            candle.low
        });
      }
    }
  }
}


/* ============================================================
   REGISTER SWEEP
   ============================================================ */

function registerSweep({
  side,
  level,
  tf,
  candle,
  sweepPrice
}) {

  const existing =
    state.recentSweeps.find(
      item =>
        item.side === side &&
        item.level === level.price &&
        item.timeframe === tf &&
        item.candleTime === candle.time
    );

  if (existing) {
    return;
  }

  const sweep = {

    side,

    level:
      level.price,

    zoneLow:
      level.priceLow,

    zoneHigh:
      level.priceHigh,

    sweepPrice,

    timeframe:
      tf,

    candleTime:
      candle.time,

    status: "SWEPT",

    reclaimed: true,

    detectedAt:
      new Date().toISOString(),

    timeframes:
      level.timeframes
  };

  state.recentSweeps.unshift(
    sweep
  );

  state.recentSweeps =
    state.recentSweeps.slice(
      0,
      50
    );
}


/* ============================================================
   LIVE CANDLE UPDATE
   ============================================================ */

function upsertCandle(
  tf,
  candle
) {

  if (
    !state.candles[tf]
  ) {
    return;
  }

  const candles =
    state.candles[tf];

  const index =
    candles.findIndex(
      c =>
        c.time === candle.time
    );

  if (index >= 0) {

    candles[index] =
      candle;

  } else {

    candles.push(
      candle
    );
  }

  const max =
    STRUCTURE[tf].candles +
    20;

  if (
    candles.length >
    max
  ) {

    state.candles[tf] =
      candles.slice(
        -max
      );
  }

  /*
     Only detect sweeps on closed candles.
  */

  if (
    candle.closed
  ) {

    detectCandleSweeps(
      tf,
      candle
    );

    rebuildStructure();
  }
}


/* ============================================================
   FUTURES KLINE WEBSOCKET
   ============================================================ */

function connectKlineWs() {

  if (
    state.klineWs &&
    (
      state.klineWs.readyState ===
      WebSocket.OPEN ||
      state.klineWs.readyState ===
      WebSocket.CONNECTING
    )
  ) {

    return;
  }

  const ws =
    new WebSocket(
      KLINE_WS
    );

  state.klineWs =
    ws;

  ws.on("open", () => {

    state.klineConnected =
      true;

    console.log(
      "Futures kline websocket connected"
    );
  });

  ws.on("message", raw => {

    try {

      const msg =
        JSON.parse(
          raw.toString()
        );

      const data =
        msg.data;

      if (
        !data ||
        data.e !== "kline"
      ) {
        return;
      }

      const k =
        data.k;

      const tf =
        k.i;

      if (
        !STRUCTURE[tf]
      ) {
        return;
      }

      upsertCandle(
        tf,
        {

          time:
            Number(k.t),

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

          closeTime:
            Number(k.T),

          closed:
            !!k.x
        }
      );

      updateCurrentPrice();

    } catch (error) {

      state.lastError =
        `Kline parse: ${error.message}`;
    }
  });

  ws.on("error", error => {

    state.lastError =
      `Kline WS: ${error.message}`;

    console.error(
      "Kline websocket error:",
      error.message
    );
  });

  ws.on("close", () => {

    state.klineConnected =
      false;

    console.log(
      "Kline websocket closed"
    );

    setTimeout(
      connectKlineWs,
      SETTINGS.klineReconnectMs
    );
  });
}


/* ============================================================
   APPLY DEPTH EVENT
   ============================================================ */

function applyDepthEvent(event) {

  if (!event) {
    return false;
  }

  const U =
    Number(event.U);

  const u =
    Number(event.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return false;
  }

  /*
     Once synchronized, this event must either:
       U <= lastUpdateId + 1 <= u

     or it is a genuine gap.
  */

  if (
    state.initialized
  ) {

    const expected =
      state.lastUpdateId + 1;

    if (
      u < expected
    ) {

      /*
         Old duplicate event.
      */

      return true;
    }

    if (
      U > expected
    ) {

      state.sequenceGaps++;

      console.warn(
        `Depth sequence gap: expected ${expected}, got U=${U}`
      );

      state.initialized =
        false;

      state.waitingForBridge =
        true;

      requestSnapshot(
        true
      );

      return false;
    }
  }

  for (
    const item of
    event.b || []
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (
      quantity === 0
    ) {

      state.bids.delete(
        String(price)
      );

    } else {

      state.bids.set(
        String(price),
        quantity
      );
    }
  }

  for (
    const item of
    event.a || []
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (
      quantity === 0
    ) {

      state.asks.delete(
        String(price)
      );

    } else {

      state.asks.set(
        String(price),
        quantity
      );
    }
  }

  state.lastUpdateId =
    u;

  updateCurrentPrice();

  state.updatedAt =
    new Date().toISOString();

  return true;
}


/* ============================================================
   DEPTH SNAPSHOT REQUEST
   ============================================================ */

function requestSnapshot(force = false) {

  const timestamp =
    now();

  if (
    state.snapshotInFlight
  ) {
    return;
  }

  if (
    !force &&
    timestamp -
      state.lastSnapshotRequestAt <
      SETTINGS.snapshotCooldownMs
  ) {

    return;
  }

  state.lastSnapshotRequestAt =
    timestamp;

  state.snapshotPending =
    true;

  state.waitingForBridge =
    true;

  state.snapshotInFlight =
    true;

  state.snapshotRequests++;

  if (
    !state.snapshotWs ||
    state.snapshotWs.readyState !==
    WebSocket.OPEN
  ) {

    connectSnapshotWs();

    state.snapshotInFlight =
      false;

    return;
  }

  const id =
    `depth-${timestamp}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  const request = {

    id,

    method: "depth",

    params: {

      symbol:
        SYMBOL,

      limit: 1000
    }
  };

  try {

    state.snapshotWs.send(
      JSON.stringify(
        request
      )
    );

  } catch (error) {

    state.lastError =
      `Snapshot send: ${error.message}`;

    state.snapshotInFlight =
      false;
  }
}


/* ============================================================
   PROCESS SNAPSHOT
   ============================================================ */

function processSnapshot(result) {

  state.snapshotInFlight =
    false;

  if (
    !result ||
    !Number.isFinite(
      Number(
        result.lastUpdateId
      )
    )
  ) {

    state.lastError =
      "Invalid Futures depth snapshot";

    return;
  }

  const snapshotId =
    Number(
      result.lastUpdateId
    );

  const bids =
    result.bids || [];

  const asks =
    result.asks || [];

  /*
     IMPORTANT:
     Do NOT apply the snapshot until we have found
     a buffered event bridging snapshotId + 1.

     This prevents a stale snapshot from creating
     a false synchronized book.
  */

  state.bridgeAttempts++;

  const bridgeIndex =
    state.pendingDepthEvents.findIndex(
      event => {

        const U =
          Number(event.U);

        const u =
          Number(event.u);

        return (
          U <= snapshotId + 1 &&
          u >= snapshotId + 1
        );
      }
    );

  if (
    bridgeIndex < 0
  ) {

    /*
       Snapshot arrived before the matching
       buffered event.

       Keep buffering and request another snapshot
       later rather than pretending synchronization
       succeeded.
    */

    state.waitingForBridge =
      true;

    state.snapshotPending =
      true;

    state.resyncs++;

    console.warn(
      `Snapshot ${snapshotId}: bridge not found`
    );

    return;
  }

  state.bridgeFound++;

  /*
     Replace local book with snapshot.
  */

  state.bids.clear();
  state.asks.clear();

  for (
    const item of bids
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(quantity) &&
      quantity > 0
    ) {

      state.bids.set(
        String(price),
        quantity
      );
    }
  }

  for (
    const item of asks
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(quantity) &&
      quantity > 0
    ) {

      state.asks.set(
        String(price),
        quantity
      );
    }
  }

  /*
     Apply bridge event.
  */

  const bridge =
    state.pendingDepthEvents[
      bridgeIndex
    ];

  applyDepthEventDirect(
    bridge
  );

  /*
     Apply all events after bridge.
  */

  const remaining =
    state.pendingDepthEvents
      .slice(
        bridgeIndex + 1
      );

  state.pendingDepthEvents =
    [];

  state.lastUpdateId =
    Number(bridge.u);

  for (
    const event of remaining
  ) {

    const U =
      Number(event.U);

    const u =
      Number(event.u);

    const expected =
      state.lastUpdateId + 1;

    if (
      u < expected
    ) {

      continue;
    }

    if (
      U > expected
    ) {

      /*
         Gap occurred while replaying.

         Keep current book temporarily but
         force a clean resync.
      */

      state.sequenceGaps++;

      state.initialized =
        false;

      state.waitingForBridge =
        true;

      state.resyncs++;

      console.warn(
        `Gap during snapshot replay: expected ${expected}, U=${U}`
      );

      requestSnapshot(
        true
      );

      return;
    }

    applyDepthEventDirect(
      event
    );
  }

  state.initialized =
    true;

  state.waitingForBridge =
    false;

  state.snapshotPending =
    false;

  state.lastUpdateId =
    state.lastUpdateId ||
    snapshotId;

  updateCurrentPrice();

  state.updatedAt =
    new Date().toISOString();

  console.log(
    `Depth synchronized at update ${state.lastUpdateId}`
  );
}


/* ============================================================
   DIRECT DEPTH APPLY
   ============================================================ */

function applyDepthEventDirect(event) {

  for (
    const item of
    event.b || []
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (
      quantity === 0
    ) {

      state.bids.delete(
        String(price)
      );

    } else {

      state.bids.set(
        String(price),
        quantity
      );
    }
  }

  for (
    const item of
    event.a || []
  ) {

    const price =
      Number(item[0]);

    const quantity =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (
      quantity === 0
    ) {

      state.asks.delete(
        String(price)
      );

    } else {

      state.asks.set(
        String(price),
        quantity
      );
    }
  }

  state.lastUpdateId =
    Number(event.u);

  updateCurrentPrice();
}


/* ============================================================
   DEPTH WEBSOCKET
   ============================================================ */

function connectDepthWs() {

  if (
    state.depthWs &&
    (
      state.depthWs.readyState ===
      WebSocket.OPEN ||
      state.depthWs.readyState ===
      WebSocket.CONNECTING
    )
  ) {

    return;
  }

  const ws =
    new WebSocket(
      DEPTH_WS
    );

  state.depthWs =
    ws;

  ws.on("open", () => {

    state.depthConnected =
      true;

    console.log(
      "Futures depth websocket connected"
    );

    /*
       Start buffering immediately.

       Snapshot synchronization will be requested
       after the stream is alive.
    */

    requestSnapshot(
      true
    );
  });

  ws.on("message", raw => {

    try {

      const event =
        JSON.parse(
          raw.toString()
        );

      if (
        !event ||
        event.e !== "depthUpdate"
      ) {
        return;
      }

      /*
         ALWAYS buffer until synchronized.

         This is the key difference from the previous
         implementation.
      */

      if (
        !state.initialized ||
        state.waitingForBridge ||
        state.snapshotPending
      ) {

        state.pendingDepthEvents.push(
          event
        );

        if (
          state.pendingDepthEvents.length >
          SETTINGS.maxPendingDepthEvents
        ) {

          state.pendingDepthEvents =
            state.pendingDepthEvents.slice(
              -SETTINGS.maxPendingDepthEvents
            );
        }

        if (
          !state.snapshotInFlight
        ) {

          requestSnapshot();
        }

        return;
      }

      applyDepthEvent(
        event
      );

    } catch (error) {

      state.lastError =
        `Depth parse: ${error.message}`;
    }
  });

  ws.on("error", error => {

    state.lastError =
      `Depth WS: ${error.message}`;

    console.error(
      "Depth websocket error:",
      error.message
    );
  });

  ws.on("close", () => {

    state.depthConnected =
      false;

    state.initialized =
      false;

    state.waitingForBridge =
      true;

    state.snapshotPending =
      true;

    state.snapshotInFlight =
      false;

    console.log(
      "Depth websocket closed"
    );

    setTimeout(
      connectDepthWs,
      SETTINGS.depthReconnectMs
    );
  });
}


/* ============================================================
   SNAPSHOT WEBSOCKET
   ============================================================ */

function connectSnapshotWs() {

  if (
    state.snapshotWs &&
    (
      state.snapshotWs.readyState ===
      WebSocket.OPEN ||
      state.snapshotWs.readyState ===
      WebSocket.CONNECTING
    )
  ) {

    return;
  }

  const ws =
    new WebSocket(
      FUTURES_API_WS
    );

  state.snapshotWs =
    ws;

  ws.on("open", () => {

    state.snapshotConnected =
      true;

    console.log(
      "Futures snapshot websocket connected"
    );

    /*
       If depth is already running, request snapshot.
    */

    if (
      state.depthConnected &&
      !state.initialized
    ) {

      requestSnapshot(
        true
      );
    }
  });

  ws.on("message", raw => {

    try {

      const message =
        JSON.parse(
          raw.toString()
        );

      if (
        message.error
      ) {

        if (
          message.error.code ===
          -1003
        ) {

          state.snapshot429s++;
        }

        state.lastError =
          `Snapshot API: ${
            message.error.msg ||
            JSON.stringify(
              message.error
            )
          }`;

        state.snapshotInFlight =
          false;

        return;
      }

      if (
        message.result
      ) {

        processSnapshot(
          message.result
        );
      }

    } catch (error) {

      state.lastError =
        `Snapshot parse: ${error.message}`;
    }
  });

  ws.on("error", error => {

    state.lastError =
      `Snapshot WS: ${error.message}`;

    console.error(
      "Snapshot websocket error:",
      error.message
    );
  });

  ws.on("close", () => {

    state.snapshotConnected =
      false;

    state.snapshotInFlight =
      false;

    console.log(
      "Snapshot websocket closed"
    );

    setTimeout(
      connectSnapshotWs,
      SETTINGS.snapshotReconnectMs
    );
  });
}


/* ============================================================
   HEALTH
   ============================================================ */

function healthResponse() {

  updateCurrentPrice();

  return {

    ok: true,

    symbol: SYMBOL,

    status:
      state.initialized
        ? "live"
        : "syncing",

    websocketConnected:
      state.depthConnected,

    depthConnected:
      state.depthConnected,

    snapshotConnected:
      state.snapshotConnected,

    klineConnected:
      state.klineConnected,

    initialized:
      state.initialized,

    waitingForBridge:
      state.waitingForBridge,

    snapshotPending:
      state.snapshotPending,

    currentPrice:
      state.currentPrice,

    priceSource:
      state.priceSource,

    bidLevels:
      state.bids.size,

    askLevels:
      state.asks.size,

    lastUpdateId:
      state.lastUpdateId,

    pendingEvents:
      state.pendingDepthEvents.length,

    snapshotRequests:
      state.snapshotRequests,

    snapshot429s:
      state.snapshot429s,

    bridgeAttempts:
      state.bridgeAttempts,

    bridgeFound:
      state.bridgeFound,

    resyncs:
      state.resyncs,

    sequenceGaps:
      state.sequenceGaps,

    structure: {

      activeLevels:
        state.structure.levels.length,

      activeMajorBSL:
        getMajorLevels("BSL").length,

      activeMajorSSL:
        getMajorLevels("SSL").length,

      lastRebuildAt:
        state.structure.lastRebuildAt,

      lastGoodAt:
        state.structure.lastGoodAt,

      lastRebuildAccepted:
        state.structure.lastRebuildAccepted,

      lastRebuildReason:
        state.structure.lastRebuildReason,

      rebuildCount:
        state.structure.rebuildCount,

      rejectedRebuilds:
        state.structure.rejectedRebuilds,

      lastCandidateLevels:
        state.structure.lastCandidateLevels,

      lastCandidateMajor:
        state.structure.lastCandidateMajor,

      retainedPrevious:
        state.structure.retainedPrevious,

      status:
        state.structure.status
    },

    updatedAt:
      state.updatedAt,

    lastError:
      state.lastError
  };
}


/* ============================================================
   BOOK ENDPOINT
   ============================================================ */

app.get(
  "/book",
  (req, res) => {

    updateCurrentPrice();

    const bids =
      getBookRows("bid")
        .sort(
          (a, b) =>
            b.price - a.price
        )
        .slice(0, 100);

    const asks =
      getBookRows("ask")
        .sort(
          (a, b) =>
            a.price - b.price
        )
        .slice(0, 100);

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      initialized:
        state.initialized,

      lastUpdateId:
        state.lastUpdateId,

      bids,

      asks,

      updatedAt:
        new Date().toISOString()
    });
  }
);


/* ============================================================
   LIQUIDITY ENDPOINT
   ============================================================ */

app.get(
  "/liquidity",
  (req, res) => {

    updateCurrentPrice();

    const orderBook =
      buildOrderBookIntelligence();

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      orderBook,

      persistentLiquidity: {

        bids:
          state.persistence.bids,

        asks:
          state.persistence.asks
      },

      updatedAt:
        new Date().toISOString()
    });
  }
);


/* ============================================================
   STRUCTURE ENDPOINT
   ============================================================ */

app.get(
  "/structure",
  (req, res) => {

    updateCurrentPrice();

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      majorLevels:
        state.structure.majorLevels,

      nearLevels:
        state.structure.nearLevels,

      allLevels:
        state.structure.levels,

      structureStatus: {

        activeLevels:
          state.structure.levels.length,

        activeMajorBSL:
          getMajorLevels("BSL").length,

        activeMajorSSL:
          getMajorLevels("SSL").length,

        lastRebuildAt:
          state.structure.lastRebuildAt,

        lastGoodAt:
          state.structure.lastGoodAt,

        lastRebuildAccepted:
          state.structure.lastRebuildAccepted,

        lastRebuildReason:
          state.structure.lastRebuildReason,

        rebuildCount:
          state.structure.rebuildCount,

        rejectedRebuilds:
          state.structure.rejectedRebuilds,

        lastCandidateLevels:
          state.structure.lastCandidateLevels,

        lastCandidateMajor:
          state.structure.lastCandidateMajor,

        retainedPrevious:
          state.structure.retainedPrevious,

        status:
          state.structure.status
      },

      updatedAt:
        new Date().toISOString()
    });
  }
);


/* ============================================================
   INTELLIGENCE ENDPOINT
   ============================================================ */

app.get(
  "/intelligence",
  (req, res) => {

    updateCurrentPrice();

    const currentPrice =
      state.currentPrice;

    const majorBSL =
      getMajorLevels("BSL");

    const majorSSL =
      getMajorLevels("SSL");

    const nearBSL =
      getNearLevels("BSL");

    const nearSSL =
      getNearLevels("SSL");

    const nearestBSL =
      majorBSL[0] ||
      null;

    const nearestSSL =
      majorSSL[0] ||
      null;

    const orderBook =
      buildOrderBookIntelligence();

    let marketState =
      "NO MAJOR STRUCTURAL LIQUIDITY";

    if (
      nearestBSL &&
      nearestSSL
    ) {

      if (
        currentPrice <
        nearestBSL.price &&
        currentPrice >
        nearestSSL.price
      ) {

        marketState =
          "BETWEEN MAJOR LIQUIDITY";

      } else if (
        currentPrice >=
        nearestBSL.price
      ) {

        marketState =
          "AT / ABOVE MAJOR BSL";

      } else if (
        currentPrice <=
        nearestSSL.price
      ) {

        marketState =
          "AT / BELOW MAJOR SSL";
      }
    }

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice,

      priceSource:
        state.priceSource,

      intelligence: {

        state:
          marketState,

        structureStatus: {

          activeLevels:
            state.structure.levels.length,

          activeMajorBSL:
            majorBSL.length,

          activeMajorSSL:
            majorSSL.length,

          lastRebuildAt:
            state.structure.lastRebuildAt,

          lastGoodAt:
            state.structure.lastGoodAt,

          lastRebuildAccepted:
            state.structure.lastRebuildAccepted,

          lastRebuildReason:
            state.structure.lastRebuildReason,

          rebuildCount:
            state.structure.rebuildCount,

          rejectedRebuilds:
            state.structure.rejectedRebuilds,

          lastCandidateLevels:
            state.structure.lastCandidateLevels,

          lastCandidateMajor:
            state.structure.lastCandidateMajor,

          retainedPrevious:
            state.structure.retainedPrevious,

          status:
            state.structure.status
        },

        nearestBSL:
          nearestBSL
            ? addDistance(
                nearestBSL
              )
            : null,

        nearestSSL:
          nearestSSL
            ? addDistance(
                nearestSSL
              )
            : null,

        nextBSL:
          majorBSL
            .slice(1, 6)
            .map(addDistance),

        nextSSL:
          majorSSL
            .slice(1, 6)
            .map(addDistance),

        nearBSL:
          nearBSL
            .slice(0, 8)
            .map(addDistance),

        nearSSL:
          nearSSL
            .slice(0, 8)
            .map(addDistance),

        orderBook,

        persistentLiquidity: {

          bids:
            state.persistence.bids,

          asks:
            state.persistence.asks
        },

        liquidityEvents:
          state.liquidityEvents.slice(
            0,
            50
          ),

        recentSweeps:
          state.recentSweeps.slice(
            0,
            20
          )
      },

      updatedAt:
        new Date().toISOString()
    });
  }
);


/* ============================================================
   DISTANCE FORMATTER
   ============================================================ */

function addDistance(level) {

  return {

    ...level,

    distancePct:
      round(
        distancePct(
          level.price,
          state.currentPrice
        ),
        3
      )
  };
}


/* ============================================================
   EVENTS ENDPOINT
   ============================================================ */

app.get(
  "/events",
  (req, res) => {

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      recentSweeps:
        state.recentSweeps,

      liquidityEvents:
        state.liquidityEvents,

      persistentLiquidity: {

        bids:
          state.persistence.bids,

        asks:
          state.persistence.asks
      },

      updatedAt:
        new Date().toISOString()
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

      ok: true,

      service:
        "BTCUSDT Major Liquidity Relay",

      version:
        "10.0",

      endpoints: [

        "/health",

        "/book",

        "/liquidity",

        "/structure",

        "/intelligence",

        "/events"
      ]
    });
  }
);


/* ============================================================
   PERIODIC PERSISTENCE
   ============================================================ */

setInterval(
  () => {

    try {

      if (
        state.initialized
      ) {

        updatePersistence();
      }

    } catch (error) {

      state.lastError =
        `Persistence: ${error.message}`;

      console.error(
        "Persistence error:",
        error.message
      );
    }

  },
  SETTINGS.persistencePollMs
);


/* ============================================================
   PERIODIC PRICE UPDATE
   ============================================================ */

setInterval(
  () => {

    try {

      updateCurrentPrice();

      /*
         Recalculate structural resting liquidity
         periodically because the book changes.
      */

      if (
        state.structure.levels.length
      ) {

        for (
          const level of
          state.structure.levels
        ) {

          getRestingLiquidity(
            level
          );
        }

        for (
          const level of
          state.structure.majorLevels
        ) {

          getRestingLiquidity(
            level
          );
        }
      }

    } catch (error) {

      state.lastError =
        `Maintenance: ${error.message}`;
    }

  },
  5000
);


/* ============================================================
   STARTUP
   ============================================================ */

async function startup() {

  console.log(
    "=============================================="
  );

  console.log(
    "BTCUSDT LIQUIDITY RELAY V10"
  );

  console.log(
    "Starting..."
  );

  console.log(
    "=============================================="
  );

  /*
     Connect websockets first so live data begins
     arriving while historical candles load.
  */

  connectDepthWs();

  connectSnapshotWs();

  connectKlineWs();

  /*
     Load historical structure.
  */

  await loadAllHistoricalCandles();

  /*
     Rebuild once more after initial price becomes
     available from the live order book.
  */

  setTimeout(
    () => {

      try {

        updateCurrentPrice();

        rebuildStructure();

      } catch (error) {

        state.lastError =
          `Initial rebuild: ${error.message}`;
      }

    },
    5000
  );
}


/* ============================================================
   SERVER
   ============================================================ */

app.listen(
  PORT,
  () => {

    console.log(
      `HTTP server listening on port ${PORT}`
    );

    startup()
      .catch(error => {

        state.lastError =
          `Startup: ${error.message}`;

        console.error(
          "Startup error:",
          error
        );
      });
  }
);
