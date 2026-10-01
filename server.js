/* ============================================================
   BINANCE BTCUSDT MAJOR LIQUIDITY RELAY
   Render + Binance Futures WebSocket
   ------------------------------------------------------------
   VERSION: 10.0

   MAJOR CHANGES
   ------------------------------------------------------------
   1. ORDER BOOK CONCENTRATION MODEL
      - Stops treating dense normal book levels as one huge wall.
      - Uses fixed USD buckets.
      - Calculates local/overall bucket baseline.
      - Requires unusually concentrated liquidity.
      - Genuine single walls can still qualify.

   2. MAXIMUM BOOK ZONE WIDTH
      - Prevents giant $100-$200+ zones from swallowing the book.

   3. PERSISTENCE CLEANUP
      - One-observation clusters are not reported as meaningful
        REMOVED liquidity.
      - Meaningful removals require multiple observations or
        sufficient lifetime.

   4. STRICTER STRUCTURAL CONFLUENCE
      - Prefers actual overlap with structural zones.
      - Otherwise requires a much tighter distance.
      - Only major structural levels can create confluence.

   5. PERSISTENCE STATE FIX
      - Correctly compares previous USD vs current USD.

   6. STABLE STRUCTURE CACHE
      - Prevents temporary rebuilds from erasing good structure.

   7. TRUE CLOSED-CANDLE SWEEP DETECTION
      - Uses the previously accepted structural map before rebuild.

   8. SAME API ENDPOINTS
      - /health
      - /book
      - /liquidity
      - /structure
      - /intelligence
      - /events
============================================================ */

const express = require("express");
const WebSocket = require("ws");

/* ============================================================
   APP
============================================================ */

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

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* ============================================================
   SETTINGS
============================================================ */

const SETTINGS = {

  /* ----------------------------------------------------------
     RAW ORDER BOOK
  ---------------------------------------------------------- */

  rawMinUsd: 50000,

  /* Price distance used when attaching resting liquidity
     to structural levels. */
  restingRadiusUsd: 35,

  /* Minimum resting USD for structural confirmation. */
  restingConfirmUsd: 250000,


  /* ----------------------------------------------------------
     STRUCTURAL ZONES
  ---------------------------------------------------------- */

  mergeUsd: 35,

  zonePct: 0.0015,

  reactionPct: 0.003,


  /* ----------------------------------------------------------
     STRUCTURAL LEVEL LIMITS
  ---------------------------------------------------------- */

  minScore: 40,

  maxLevels: 8,


  /* ----------------------------------------------------------
     ORDER BOOK CONCENTRATION
     
     IMPORTANT:
     We no longer simply connect every adjacent order.
  ---------------------------------------------------------- */

  bookBucketUsd: 10,

  /* Absolute minimum bucket size. */
  orderBucketMinUsd: 250000,

  /* A single bucket can qualify if it is this large. */
  orderSingleBucketMinUsd: 1000000,

  /* Total concentrated liquidity required. */
  orderMinUsd: 750000,

  /* Minimum number of strong buckets for a cluster. */
  minStrongBuckets: 2,

  /* Maximum physical width of one book cluster. */
  orderMaxClusterWidthUsd: 150,

  /* Maximum bucket gap while clustering. */
  orderMaxBucketGapUsd: 10,

  /* Concentration multiplier over median bucket. */
  orderConcentrationMultiple: 2.5,

  /* Percentile floor used as a second concentration filter. */
  orderPercentile: 0.75,

  /* Only return strongest book zones. */
  maxBookClusters: 8,


  /* ----------------------------------------------------------
     PERSISTENCE
  ---------------------------------------------------------- */

  persistencePollMs: 5000,

  persistenceConfirmMs: 60000,

  persistenceMaxAgeMs: 10 * 60 * 1000,

  persistenceTolerancePct: 0.0005,

  persistenceMinObservationsForRemoval: 2,

  persistenceMinLifetimeMs: 10000,


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

  confluenceMaxDistanceUsd: 100,

  confluenceMaxDistancePct: 0.0012,


  /* ----------------------------------------------------------
     BOOK EVENT FILTERS
  ---------------------------------------------------------- */

  eventMinPersistentUsd: 750000,

  eventStrengthChangePct: 0.20
};


/* ============================================================
   STRUCTURE SETTINGS
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
   STATE
============================================================ */

const state = {

  currentPrice: null,

  priceSource: null,

  bids: new Map(),

  asks: new Map(),

  depthConnected: false,

  depthInitialized: false,

  depthLastUpdateId: 0,

  depthPendingEvents: [],

  depthSnapshotPending: false,

  depthSnapshotRequests: 0,

  depthSnapshot429s: 0,

  depthBridgeAttempts: 0,

  depthBridgeFound: 0,

  depthResyncs: 0,

  depthSequenceGaps: 0,

  lastDepthEventAt: null,

  lastSnapshotAt: null,

  lastError: null,

  lastBookUpdateAt: null

};


/* ============================================================
   CANDLE STATE
============================================================ */

const candles = {

  "15m": [],

  "1h": [],

  "4h": [],

  "1d": []

};


/* ============================================================
   STRUCTURE STATE
============================================================ */

const structure = {

  swingHighs: [],

  swingLows: [],

  equalHighs: [],

  equalLows: [],

  levels: [],

  majorLevels: [],

  nearLevels: [],

  lastGoodLevels: [],

  lastGoodMajorLevels: [],

  lastAttemptAt: null,

  lastGoodAt: null,

  lastRebuildAt: null,

  lastRebuildAccepted: false,

  lastRebuildReason: null,

  lastCandidateLevels: 0,

  lastCandidateMajor: 0,

  rebuildCount: 0,

  rejectedRebuilds: 0,

  retainedPrevious: false,

  status: "INITIALIZING"

};


/* ============================================================
   PERSISTENCE
============================================================ */

const persistence = {

  bids: new Map(),

  asks: new Map(),

  lastPollAt: null

};


/* ============================================================
   EVENTS
============================================================ */

const liquidityEvents = [];

const sweepEvents = [];


/* ============================================================
   KLINE CONNECTION
============================================================ */

let klineSocket = null;


/* ============================================================
   DEPTH CONNECTION
============================================================ */

let depthSocket = null;

let depthReconnectTimer = null;


/* ============================================================
   UTILITY
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


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


function safeJson(value) {

  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }

}


function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}


function median(values) {

  if (!values.length) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);

  const middle = Math.floor(sorted.length / 2);

  if (sorted.length % 2) {
    return sorted[middle];
  }

  return (sorted[middle - 1] + sorted[middle]) / 2;
}


function percentile(values, p) {

  if (!values.length) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);

  const index = (sorted.length - 1) * p;

  const lower = Math.floor(index);

  const upper = Math.ceil(index);

  if (lower === upper) {
    return sorted[lower];
  }

  const weight = index - lower;

  return (
    sorted[lower] * (1 - weight) +
    sorted[upper] * weight
  );
}


function normalizeSide(side) {

  if (side === "bid" || side === "bids" || side === "SSL") {
    return "bid";
  }

  return "ask";
}


function structuralSideToBookSide(side) {

  return side === "BSL" ? "ask" : "bid";
}


function bookSideToStructuralSide(side) {

  return side === "ask" ? "BSL" : "SSL";
}


/* ============================================================
   HTTP FETCH
============================================================ */

async function fetchJson(url) {

  const response = await fetch(url);

  if (!response.ok) {

    const text = await response.text();

    throw new Error(
      `HTTP ${response.status}: ${text.slice(0, 300)}`
    );
  }

  return response.json();
}


/* ============================================================
   SPOT HISTORICAL CANDLES
============================================================ */

async function fetchSpotKlines(interval, limit) {

  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${limit}`;

  const data = await fetchJson(url);

  return data.map(k => ({

    openTime: Number(k[0]),

    open: Number(k[1]),

    high: Number(k[2]),

    low: Number(k[3]),

    close: Number(k[4]),

    volume: Number(k[5]),

    closeTime: Number(k[6])

  }));
}


/* ============================================================
   LOAD HISTORY
============================================================ */

async function loadHistory() {

  for (const [tf, cfg] of Object.entries(STRUCTURE)) {

    try {

      const data = await fetchSpotKlines(
        cfg.interval,
        cfg.candles
      );

      candles[tf] = data;

    } catch (error) {

      state.lastError =
        `History ${tf}: ${error.message}`;

      console.error(
        `[HISTORY ${tf}]`,
        error.message
      );

    }

  }

  rebuildStructure("history");

}


/* ============================================================
   PIVOT DETECTION
============================================================ */

function detectPivots(data, left, right) {

  const highs = [];

  const lows = [];

  if (!data || data.length < left + right + 1) {

    return {
      highs,
      lows
    };

  }


  for (
    let i = left;
    i < data.length - right;
    i++
  ) {

    const candle = data[i];

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

      if (data[j].high >= candle.high) {
        isHigh = false;
      }

      if (data[j].low <= candle.low) {
        isLow = false;
      }

    }


    if (isHigh) {

      highs.push({

        price: candle.high,

        time: candle.openTime,

        index: i

      });

    }


    if (isLow) {

      lows.push({

        price: candle.low,

        time: candle.openTime,

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
   EQUAL CLUSTERING
============================================================ */

function clusterPivots(pivots, tolerancePct) {

  if (!pivots.length) {
    return [];
  }

  const sorted = [...pivots]
    .sort((a, b) => a.price - b.price);

  const clusters = [];


  for (const pivot of sorted) {

    let target = null;

    for (const cluster of clusters) {

      const midpoint =
        (
          cluster.min +
          cluster.max
        ) / 2;

      const tolerance =
        midpoint * tolerancePct;

      if (
        Math.abs(
          pivot.price - midpoint
        ) <= tolerance
      ) {

        target = cluster;

        break;

      }

    }


    if (!target) {

      target = {

        min: pivot.price,

        max: pivot.price,

        prices: [],

        pivots: []

      };

      clusters.push(target);

    }


    target.min =
      Math.min(
        target.min,
        pivot.price
      );

    target.max =
      Math.max(
        target.max,
        pivot.price
      );

    target.prices.push(pivot.price);

    target.pivots.push(pivot);

  }


  return clusters.map(cluster => ({

    price:
      cluster.prices.reduce(
        (a, b) => a + b,
        0
      ) / cluster.prices.length,

    priceLow: cluster.min,

    priceHigh: cluster.max,

    equalCount: cluster.prices.length,

    pivots: cluster.pivots

  }));

}


/* ============================================================
   REACTION COUNT
============================================================ */

function reactionCount(data, pivotIndex, side) {

  if (!data || !data.length) {
    return 0;
  }

  const pivot = data[pivotIndex];

  if (!pivot) {
    return 0;
  }

  const reactionWindow = 8;

  const start =
    Math.min(
      data.length - 1,
      pivotIndex + 1
    );

  const end =
    Math.min(
      data.length - 1,
      pivotIndex + reactionWindow
    );

  let reactions = 0;


  if (side === "high") {

    for (let i = start; i <= end; i++) {

      if (
        data[i].close <
        pivot.high *
        (1 - SETTINGS.reactionPct)
      ) {

        reactions++;

      }

    }

  } else {

    for (let i = start; i <= end; i++) {

      if (
        data[i].close >
        pivot.low *
        (1 + SETTINGS.reactionPct)
      ) {

        reactions++;

      }

    }

  }


  return reactions;

}


/* ============================================================
   BUILD STRUCTURAL LEVELS
============================================================ */

function buildStructureLevels() {

  const highs = [];

  const lows = [];

  const equalHighs = [];

  const equalLows = [];


  for (const [tf, cfg] of Object.entries(STRUCTURE)) {

    const data = candles[tf];

    if (!data || data.length < 20) {
      continue;
    }


    const pivots =
      detectPivots(
        data,
        cfg.left,
        cfg.right
      );


    const highClusters =
      clusterPivots(
        pivots.highs,
        cfg.eq
      );


    const lowClusters =
      clusterPivots(
        pivots.lows,
        cfg.eq
      );


    for (const cluster of highClusters) {

      let reactions = 0;

      for (const pivot of cluster.pivots) {

        reactions +=
          reactionCount(
            data,
            pivot.index,
            "high"
          );

      }


      highs.push({

        side: "BSL",

        price: cluster.price,

        priceLow: cluster.priceLow,

        priceHigh: cluster.priceHigh,

        equalCount: cluster.equalCount,

        reactions,

        timeframe: tf,

        timeframeWeight: cfg.weight

      });


      equalHighs.push({

        ...cluster,

        timeframe: tf

      });

    }


    for (const cluster of lowClusters) {

      let reactions = 0;

      for (const pivot of cluster.pivots) {

        reactions +=
          reactionCount(
            data,
            pivot.index,
            "low"
          );

      }


      lows.push({

        side: "SSL",

        price: cluster.price,

        priceLow: cluster.priceLow,

        priceHigh: cluster.priceHigh,

        equalCount: cluster.equalCount,

        reactions,

        timeframe: tf,

        timeframeWeight: cfg.weight

      });


      equalLows.push({

        ...cluster,

        timeframe: tf

      });

    }

  }


  return {

    highs,

    lows,

    equalHighs,

    equalLows

  };

}


/* ============================================================
   MERGE ACROSS TIMEFRAMES
============================================================ */

function mergeAcrossTimeframes(levels) {

  const sorted =
    [...levels]
      .sort(
        (a, b) =>
          a.price -
          b.price
      );


  const merged = [];


  for (const level of sorted) {

    let target = null;


    for (const existing of merged) {

      const distance =
        Math.abs(
          level.price -
          existing.price
        );


      if (
        distance <=
        SETTINGS.mergeUsd
      ) {

        target = existing;

        break;

      }

    }


    if (!target) {

      target = {

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

        strength: "LOW",

        restingUsd: 0,

        restingConfirmed: false,

        restingLevels: []

      };

      merged.push(target);

      continue;

    }


    target.price =
      (
        target.price +
        level.price
      ) / 2;


    target.priceLow =
      Math.min(
        target.priceLow,
        level.priceLow
      );


    target.priceHigh =
      Math.max(
        target.priceHigh,
        level.priceHigh
      );


    target.equalCount +=
      level.equalCount;


    target.reactions +=
      level.reactions;


    if (
      !target.timeframes.includes(
        level.timeframe
      )
    ) {

      target.timeframes.push(
        level.timeframe
      );

    }


    target.timeframeWeight +=
      level.timeframeWeight;

  }


  for (const level of merged) {

    level.scoreBase =
      level.timeframeWeight * 10 +
      Math.min(
        level.equalCount,
        30
      ) * 5 +
      Math.min(
        level.reactions,
        40
      ) * 2;

    level.score =
      level.scoreBase;


    if (
      level.score >= 125 ||
      (
        level.timeframes.length >= 3 &&
        level.equalCount >= 4
      )
    ) {

      level.strength = "HIGH";

    } else if (
      level.score >= 90 ||
      level.timeframes.length >= 2 ||
      level.equalCount >= 3
    ) {

      level.strength = "MEDIUM";

    } else {

      level.strength = "LOW";

    }

  }


  return merged;

}


/* ============================================================
   TIMEFRAME WEIGHT
============================================================ */

function timeframeWeight(level) {

  if (
    Number.isFinite(
      level.timeframeWeight
    )
  ) {

    return level.timeframeWeight;

  }


  return (
    level.timeframes || []
  ).reduce(
    (sum, tf) =>
      sum +
      (
        STRUCTURE[tf]?.weight ||
        0
      ),
    0
  );

}


/* ============================================================
   MAJOR LEVEL TEST
============================================================ */

function isMajorLevel(level) {

  if (
    !state.currentPrice ||
    !Number.isFinite(
      state.currentPrice
    )
  ) {

    return false;

  }


  const distancePct =
    Math.abs(
      level.price -
      state.currentPrice
    ) /
    state.currentPrice *
    100;


  if (
    distancePct <
    SETTINGS.majorMinDistancePct
  ) {

    return false;

  }


  const weight =
    timeframeWeight(level);


  if (
    weight <
    SETTINGS.majorMinimumTimeframeWeight
  ) {

    return false;

  }


  const tfs =
    level.timeframes || [];


  const only15m =
    tfs.length === 1 &&
    tfs[0] === "15m";


  if (only15m) {
    return false;
  }


  const hasHigherTF =
    tfs.includes("4h") ||
    tfs.includes("1d");


  const has1h =
    tfs.includes("1h");


  if (
    has1h &&
    !hasHigherTF &&
    level.score <
    SETTINGS.major1hMinScore
  ) {

    return false;

  }


  if (
    hasHigherTF &&
    level.score <
    SETTINGS.majorHigherTfMinScore
  ) {

    return false;

  }


  return true;

}


/* ============================================================
   CLONE LEVELS
============================================================ */

function cloneLevels(levels) {

  return safeJson(
    levels || []
  );

}


/* ============================================================
   APPLY RESTING LIQUIDITY
============================================================ */

function applyRestingToLevels(levels) {

  if (!levels.length) {
    return levels;
  }


  for (const level of levels) {

    level.restingUsd = 0;

    level.restingConfirmed = false;

    level.restingLevels = [];


    const bookSide =
      structuralSideToBookSide(
        level.side
      );


    const rows =
      bookRows(
        bookSide
      );


    for (const row of rows) {

      const distance =
        Math.min(
          Math.abs(
            row.price -
            level.priceLow
          ),
          Math.abs(
            row.price -
            level.priceHigh
          ),
          Math.abs(
            row.price -
            level.price
          )
        );


      if (
        distance <=
        SETTINGS.restingRadiusUsd
      ) {

        level.restingUsd +=
          row.usd;

        level.restingLevels.push(
          row
        );

      }

    }


    if (
      level.restingUsd >=
      SETTINGS.restingConfirmUsd
    ) {

      level.restingConfirmed = true;

      level.score =
        level.scoreBase +
        Math.min(
          25,
          Math.round(
            level.restingUsd /
            500000
          ) * 5
        );

    } else {

      level.score =
        level.scoreBase;

    }

  }


  return levels;

}


/* ============================================================
   MAJOR / NEAR
============================================================ */

function isNearLevel(level) {

  if (
    !state.currentPrice ||
    !Number.isFinite(
      state.currentPrice
    )
  ) {

    return false;

  }


  const distancePct =
    Math.abs(
      level.price -
      state.currentPrice
    ) /
    state.currentPrice *
    100;


  return (
    distancePct <
    SETTINGS.majorMinDistancePct
  );

}


/* ============================================================
   STRUCTURAL REBUILD
============================================================ */

function rebuildStructure(reason = "manual") {

  structure.lastAttemptAt =
    new Date().toISOString();

  structure.lastRebuildAt =
    structure.lastAttemptAt;

  structure.rebuildCount++;


  const raw =
    buildStructureLevels();


  let levels =
    mergeAcrossTimeframes(
      [
        ...raw.highs,
        ...raw.lows
      ]
    );


  structure.swingHighs =
    raw.highs;

  structure.swingLows =
    raw.lows;

  structure.equalHighs =
    raw.equalHighs;

  structure.equalLows =
    raw.equalLows;


  structure.lastCandidateLevels =
    levels.length;


  /* ----------------------------------------------------------
     Apply current order-book confirmation
  ---------------------------------------------------------- */

  levels =
    applyRestingToLevels(
      levels
    );


  const candidateMajor =
    levels.filter(
      isMajorLevel
    );


  structure.lastCandidateMajor =
    candidateMajor.length;


  /* ----------------------------------------------------------
     Decide whether this rebuild is acceptable.
  ---------------------------------------------------------- */

  const previousAge =
    structure.lastGoodAt
      ? now() -
        new Date(
          structure.lastGoodAt
        ).getTime()
      : Infinity;


  let accepted = true;

  let acceptedLevels = levels;

  let acceptedMajor =
    candidateMajor;


  let rebuildReason =
    "valid_structure";


  if (
    state.currentPrice &&
    candidateMajor.length === 0 &&
    structure.lastGoodLevels.length &&
    previousAge <
      SETTINGS.structureCacheMaxAgeMs
  ) {

    const stillRelevant =
      structure.lastGoodLevels.filter(
        isMajorLevel
      );


    if (stillRelevant.length) {

      accepted = false;

      structure.rejectedRebuilds++;

      structure.retainedPrevious = true;

      rebuildReason =
        "retained_previous_major_structure";


      acceptedLevels =
        cloneLevels(
          structure.lastGoodLevels
        );


      acceptedLevels =
        applyRestingToLevels(
          acceptedLevels
        );


      acceptedMajor =
        acceptedLevels.filter(
          isMajorLevel
        );

    }

  }


  if (accepted) {

    structure.retainedPrevious = false;

    structure.lastGoodAt =
      new Date().toISOString();

    structure.lastGoodLevels =
      cloneLevels(
        acceptedLevels
      );

    structure.lastGoodMajorLevels =
      cloneLevels(
        acceptedMajor
      );

    structure.lastRebuildAccepted =
      true;

    structure.status =
      acceptedMajor.length
        ? "HEALTHY"
        : "VALID_NO_MAJOR_STRUCTURE";

  } else {

    structure.lastRebuildAccepted =
      false;

    structure.status =
      "STALE_FALLBACK";

  }


  structure.lastRebuildReason =
    rebuildReason;


  structure.levels =
    acceptedLevels;


  structure.majorLevels =
    acceptedMajor;


  structure.nearLevels =
    acceptedLevels.filter(
      isNearLevel
    );


  /* ----------------------------------------------------------
     If accepted structure has no major levels and no previous
     cache, this is still a valid structural state.
  ---------------------------------------------------------- */

  if (
    acceptedMajor.length === 0 &&
    !structure.lastGoodMajorLevels.length
  ) {

    structure.status =
      "VALID_NO_MAJOR_STRUCTURE";

  }


  return structure;

}


/* ============================================================
   RAW BOOK ROWS
============================================================ */

function bookRows(side) {

  const source =
    normalizeSide(side) === "bid"
      ? state.bids
      : state.asks;


  const rows = [];


  for (
    const [
      price,
      quantity
    ] of source.entries()
  ) {

    const usd =
      price *
      quantity;


    if (
      usd <
      SETTINGS.rawMinUsd
    ) {

      continue;

    }


    rows.push({

      price,

      quantity,

      usd

    });

  }


  rows.sort(
    (a, b) =>
      a.price -
      b.price
  );


  return rows;

}


/* ============================================================
   ORDER BOOK BUCKETS
============================================================ */

function buildBookBuckets(side) {

  const rows =
    bookRows(side);


  const bucketSize =
    SETTINGS.bookBucketUsd;


  const buckets =
    new Map();


  for (const row of rows) {

    const bucketPrice =
      Math.floor(
        row.price /
        bucketSize
      ) *
      bucketSize;


    const key =
      bucketPrice.toFixed(2);


    if (!buckets.has(key)) {

      buckets.set(
        key,
        {
          priceLow: bucketPrice,
          priceHigh:
            bucketPrice +
            bucketSize,
          midpoint:
            bucketPrice +
            bucketSize / 2,
          usd: 0,
          levels: 0,
          strongestPrice:
            row.price,
          strongestUsd:
            row.usd,
          rows: []
        }
      );

    }


    const bucket =
      buckets.get(key);


    bucket.usd +=
      row.usd;

    bucket.levels++;

    bucket.rows.push(
      row
    );


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
        a.midpoint -
        b.midpoint
    );

}


/* ============================================================
   ORDER BOOK CONCENTRATION CLUSTERS
============================================================ */

function concentratedBookClusters(side) {

  const buckets =
    buildBookBuckets(side);


  if (!buckets.length) {
    return [];
  }


  const bucketValues =
    buckets.map(
      bucket =>
        bucket.usd
    );


  const medianUsd =
    median(bucketValues);


  const percentileUsd =
    percentile(
      bucketValues,
      SETTINGS.orderPercentile
    );


  const concentrationThreshold =
    Math.max(
      SETTINGS.orderBucketMinUsd,
      medianUsd *
        SETTINGS.orderConcentrationMultiple,
      percentileUsd
    );


  /* ----------------------------------------------------------
     Strong bucket definition.

     A bucket is strong when:
       - it exceeds the absolute floor
       - AND it is unusually concentrated

     A genuine single wall can bypass the relative test.
  ---------------------------------------------------------- */

  const strongBuckets =
    buckets.filter(
      bucket => {

        const relativeStrong =
          bucket.usd >=
          concentrationThreshold;

        const absoluteWall =
          bucket.usd >=
          SETTINGS.orderSingleBucketMinUsd;

        return (
          relativeStrong ||
          absoluteWall
        );

      }
    );


  if (!strongBuckets.length) {
    return [];
  }


  /* ----------------------------------------------------------
     Group only adjacent strong buckets.

     Normal book buckets that are not strong are NOT included.
     This is the key fix for the old giant clusters.
  ---------------------------------------------------------- */

  const groups = [];

  let current = null;


  for (const bucket of strongBuckets) {

    if (!current) {

      current = [bucket];

      continue;

    }


    const previous =
      current[
        current.length - 1
      ];


    const gap =
      bucket.priceLow -
      previous.priceHigh;


    const newWidth =
      bucket.priceHigh -
      current[0].priceLow;


    if (
      gap <=
      SETTINGS.orderMaxBucketGapUsd &&
      newWidth <=
      SETTINGS.orderMaxClusterWidthUsd
    ) {

      current.push(bucket);

    } else {

      groups.push(
        current
      );

      current = [bucket];

    }

  }


  if (current) {

    groups.push(
      current
    );

  }


  /* ----------------------------------------------------------
     Convert groups into liquidity zones.
  ---------------------------------------------------------- */

  const clusters = [];


  for (const group of groups) {

    const usd =
      group.reduce(
        (sum, bucket) =>
          sum +
          bucket.usd,
        0
      );


    const strongest =
      group.reduce(
        (best, bucket) =>
          bucket.strongestUsd >
          best.strongestUsd
            ? bucket
            : best
      );


    const strongBucketCount =
      group.length;


    const width =
      group[group.length - 1]
        .priceHigh -
      group[0]
        .priceLow;


    const qualifies =
      usd >=
        SETTINGS.orderMinUsd &&
      (
        strongBucketCount >=
          SETTINGS.minStrongBuckets ||
        strongest.strongestUsd >=
          SETTINGS.orderSingleBucketMinUsd
      );


    if (!qualifies) {
      continue;
    }


    clusters.push({

      side,

      priceLow:
        group[0].priceLow,

      priceHigh:
        group[group.length - 1]
          .priceHigh,

      midpoint:
        group.reduce(
          (sum, bucket) =>
            sum +
            bucket.midpoint *
            bucket.usd,
          0
        ) /
        usd,

      usd,

      levels:
        group.reduce(
          (sum, bucket) =>
            sum +
            bucket.levels,
          0
        ),

      strongBuckets:
        strongBucketCount,

      strongestPrice:
        strongest.strongestPrice,

      strongestBucketUsd:
        strongest.strongestUsd,

      medianBucketUsd:
        medianUsd,

      concentrationThreshold,

      width

    });

  }


  clusters.sort(
    (a, b) =>
      b.usd -
      a.usd
  );


  return clusters.slice(
    0,
    SETTINGS.maxBookClusters
  );

}


/* ============================================================
   ORDER BOOK
============================================================ */

function getBook() {

  return {

    bids:
      concentratedBookClusters(
        "bid"
      ),

    asks:
      concentratedBookClusters(
        "ask"
      )

  };

}


/* ============================================================
   MAJOR STRUCTURAL LEVELS
============================================================ */

function structural(side) {

  return structure.levels

    .filter(
      level =>
        level.side === side
    )

    .filter(
      isMajorLevel
    )

    .sort(
      (a, b) => {

        if (
          side === "BSL"
        ) {

          return (
            a.price -
            b.price
          );

        }

        return (
          b.price -
          a.price
        );

      }

    );

}


/* ============================================================
   NEAR STRUCTURAL LEVELS
============================================================ */

function nearStructural(side) {

  return structure.levels

    .filter(
      level =>
        level.side === side
    )

    .filter(
      isNearLevel
    )

    .sort(
      (a, b) => {

        const da =
          Math.abs(
            a.price -
            state.currentPrice
          );

        const db =
          Math.abs(
            b.price -
            state.currentPrice
          );

        return da - db;

      }

    );

}


/* ============================================================
   STRICT BOOK / STRUCTURAL CONFLUENCE
============================================================ */

function structuralNearBookCluster(
  cluster
) {

  if (
    !state.currentPrice
  ) {

    return null;

  }


  const bookLow =
    cluster.priceLow;

  const bookHigh =
    cluster.priceHigh;


  const majors =
    structure.levels.filter(
      isMajorLevel
    );


  let best = null;


  for (const level of majors) {

    if (
      structuralSideToBookSide(
        level.side
      ) !==
      cluster.side
    ) {

      continue;

    }


    /* --------------------------------------------------------
       BEST CASE:
       Actual zone overlap.
    -------------------------------------------------------- */

    const overlap =
      bookHigh >=
        level.priceLow &&
      bookLow <=
        level.priceHigh;


    if (overlap) {

      const overlapLow =
        Math.max(
          bookLow,
          level.priceLow
        );

      const overlapHigh =
        Math.min(
          bookHigh,
          level.priceHigh
        );


      const overlapWidth =
        Math.max(
          0,
          overlapHigh -
          overlapLow
        );


      const levelWidth =
        Math.max(
          1,
          level.priceHigh -
          level.priceLow
        );


      const overlapRatio =
        overlapWidth /
        levelWidth;


      const candidate = {

        level,

        overlap: true,

        overlapRatio,

        distance: 0

      };


      if (
        !best ||
        candidate.overlapRatio >
          best.overlapRatio
      ) {

        best = candidate;

      }

      continue;

    }


    /* --------------------------------------------------------
       NON-OVERLAP:
       Require tight distance.
    -------------------------------------------------------- */

    let distance = Infinity;


    if (
      bookHigh <
      level.priceLow
    ) {

      distance =
        level.priceLow -
        bookHigh;

    } else if (
      bookLow >
      level.priceHigh
    ) {

      distance =
        bookLow -
        level.priceHigh;

    }


    const maxDistance =
      Math.min(
        SETTINGS.confluenceMaxDistanceUsd,
        level.price *
          SETTINGS.confluenceMaxDistancePct
      );


    if (
      distance <=
      maxDistance
    ) {

      const candidate = {

        level,

        overlap: false,

        overlapRatio: 0,

        distance

      };


      if (
        !best ||
        candidate.distance <
          best.distance
      ) {

        best = candidate;

      }

    }

  }


  return best;

}


/* ============================================================
   APPLY BOOK TO INTELLIGENCE
============================================================ */

function enrichBookClusters(
  clusters
) {

  return clusters.map(
    cluster => {

      const confluence =
        structuralNearBookCluster(
          cluster
        );


      return {

        ...cluster,

        structuralConfluence:
          Boolean(confluence),

        structuralLevel:
          confluence
            ? confluence.level.price
            : null,

        structuralZoneLow:
          confluence
            ? confluence.level.priceLow
            : null,

        structuralZoneHigh:
          confluence
            ? confluence.level.priceHigh
            : null,

        structuralTimeframes:
          confluence
            ? confluence.level.timeframes
            : [],

        structuralOverlap:
          confluence
            ? confluence.overlap
            : false,

        structuralOverlapRatio:
          confluence
            ? round(
                confluence.overlapRatio,
                3
              )
            : 0

      };

    }
  );

}


/* ============================================================
   PERSISTENCE CLUSTER MATCH
============================================================ */

function clustersStillNear(
  oldCluster,
  newCluster
) {

  const tolerance =
    Math.max(
      20,
      state.currentPrice *
        SETTINGS.persistenceTolerancePct
    );


  const oldLow =
    oldCluster.priceLow;

  const oldHigh =
    oldCluster.priceHigh;


  const newLow =
    newCluster.priceLow;

  const newHigh =
    newCluster.priceHigh;


  const overlap =
    newHigh >= oldLow &&
    newLow <= oldHigh;


  if (overlap) {
    return true;
  }


  return (
    Math.abs(
      oldCluster.midpoint -
      newCluster.midpoint
    ) <=
    tolerance
  );

}


/* ============================================================
   PERSISTENCE STATE
============================================================ */

function getPersistenceStatus(
  previousUsd,
  currentUsd,
  ageMs
) {

  if (
    ageMs <
    SETTINGS.persistenceConfirmMs
  ) {

    return "NEW";

  }


  if (
    previousUsd > 0 &&
    currentUsd >
      previousUsd *
      (1 +
        SETTINGS.eventStrengthChangePct)
  ) {

    return "STRENGTHENING";

  }


  if (
    previousUsd > 0 &&
    currentUsd <
      previousUsd *
      (1 -
        SETTINGS.eventStrengthChangePct)
  ) {

    return "WEAKENING";

  }


  return "HOLDING";

}


/* ============================================================
   EVENT PUSH
============================================================ */

function pushLiquidityEvent(event) {

  liquidityEvents.unshift(
    event
  );


  while (
    liquidityEvents.length >
    SETTINGS.eventMax
  ) {

    liquidityEvents.pop();

  }

}


/* ============================================================
   EVENT SIDE
============================================================ */

function eventSide(
  bookSide
) {

  return bookSide === "ask"
    ? "BSL"
    : "SSL";

}


/* ============================================================
   CREATE PERSISTENCE ITEM
============================================================ */

function createPersistenceItem(
  cluster
) {

  const timestamp =
    now();


  return {

    ...safeJson(cluster),

    firstSeen:
      new Date(
        timestamp
      ).toISOString(),

    lastSeen:
      new Date(
        timestamp
      ).toISOString(),

    durationMs: 0,

    durationSec: 0,

    observations: 1,

    maxUsd:
      cluster.usd,

    previousUsd: 0,

    changeUsd: 0,

    status: "NEW",

    removed: false,

    persistent: false,

    structuralConfluence:
      Boolean(
        cluster.structuralConfluence
      ),

    structuralLevel:
      cluster.structuralLevel ||
      null,

    structuralZoneLow:
      cluster.structuralZoneLow ||
      null,

    structuralZoneHigh:
      cluster.structuralZoneHigh ||
      null,

    structuralTimeframes:
      cluster.structuralTimeframes ||
      [],

    persistentEventSent: false,

    confluenceEventSent: false,

    strengthEventAt: 0,

    removedEventSent: false

  };

}


/* ============================================================
   UPDATE PERSISTENCE
============================================================ */

function updatePersistenceSide(
  side,
  clusters
) {

  const map =
    side === "bid"
      ? persistence.bids
      : persistence.asks;


  const matchedIds =
    new Set();


  /* ----------------------------------------------------------
     MATCH EXISTING CLUSTERS
  ---------------------------------------------------------- */

  for (const cluster of clusters) {

    let matchId = null;

    let bestDistance =
      Infinity;


    for (
      const [
        id,
        existing
      ] of map.entries()
    ) {

      if (
        existing.removed
      ) {

        continue;

      }


      if (
        !clustersStillNear(
          existing,
          cluster
        )
      ) {

        continue;

      }


      const distance =
        Math.abs(
          existing.midpoint -
          cluster.midpoint
        );


      if (
        distance <
        bestDistance
      ) {

        bestDistance =
          distance;

        matchId =
          id;

      }

    }


    if (matchId) {

      const item =
        map.get(
          matchId
        );


      const previousUsd =
        item.usd;


      item.previousUsd =
        previousUsd;


      item.usd =
        cluster.usd;


      item.priceLow =
        cluster.priceLow;


      item.priceHigh =
        cluster.priceHigh;


      item.midpoint =
        cluster.midpoint;


      item.strongestPrice =
        cluster.strongestPrice;


      item.strongestBucketUsd =
        cluster.strongestBucketUsd;


      item.levels =
        cluster.levels;


      item.strongBuckets =
        cluster.strongBuckets;


      item.medianBucketUsd =
        cluster.medianBucketUsd;


      item.concentrationThreshold =
        cluster.concentrationThreshold;


      item.lastSeen =
        new Date(
          now()
        ).toISOString();


      item.durationMs =
        now() -
        new Date(
          item.firstSeen
        ).getTime();


      item.durationSec =
        Math.floor(
          item.durationMs /
          1000
        );


      item.observations++;


      item.maxUsd =
        Math.max(
          item.maxUsd,
          item.usd
        );


      item.changeUsd =
        item.usd -
        previousUsd;


      item.status =
        getPersistenceStatus(
          previousUsd,
          item.usd,
          item.durationMs
        );


      item.persistent =
        item.durationMs >=
        SETTINGS.persistenceConfirmMs;


      const newConfluence =
        structuralNearBookCluster(
          cluster
        );


      if (newConfluence) {

        item.structuralConfluence =
          true;

        item.structuralLevel =
          newConfluence.level.price;

        item.structuralZoneLow =
          newConfluence.level.priceLow;

        item.structuralZoneHigh =
          newConfluence.level.priceHigh;

        item.structuralTimeframes =
          newConfluence.level.timeframes;

      } else {

        item.structuralConfluence =
          false;

        item.structuralLevel =
          null;

        item.structuralZoneLow =
          null;

        item.structuralZoneHigh =
          null;

        item.structuralTimeframes =
          [];

      }


      matchedIds.add(
        matchId
      );


      /* ------------------------------------------------------
         Persistent event
      ------------------------------------------------------ */

      if (
        item.persistent &&
        !item.persistentEventSent &&
        item.usd >=
          SETTINGS.eventMinPersistentUsd
      ) {

        item.persistentEventSent =
          true;


        pushLiquidityEvent({

          key:
            `PERSISTENT|${matchId}`,

          type:
            "PERSISTENT",

          time:
            now(),

          side:
            eventSide(side),

          bookPriceLow:
            item.priceLow,

          bookPriceHigh:
            item.priceHigh,

          currentUsd:
            item.usd,

          maxUsd:
            item.maxUsd,

          durationMs:
            item.durationMs,

          lifetimeSec:
            item.durationSec,

          observations:
            item.observations,

          structuralLevel:
            item.structuralLevel,

          structuralZoneLow:
            item.structuralZoneLow,

          structuralZoneHigh:
            item.structuralZoneHigh,

          timeframes:
            item.structuralTimeframes,

          message:
            "PERSISTENT LIQUIDITY"

        });

      }


      /* ------------------------------------------------------
         Structural confluence event
      ------------------------------------------------------ */

      if (
        item.persistent &&
        item.structuralConfluence &&
        !item.confluenceEventSent
      ) {

        item.confluenceEventSent =
          true;


        pushLiquidityEvent({

          key:
            `CONFLUENCE|${matchId}`,

          type:
            "CONFLUENCE",

          time:
            now(),

          side:
            eventSide(side),

          bookPriceLow:
            item.priceLow,

          bookPriceHigh:
            item.priceHigh,

          currentUsd:
            item.usd,

          maxUsd:
            item.maxUsd,

          durationMs:
            item.durationMs,

          lifetimeSec:
            item.durationSec,

          observations:
            item.observations,

          structuralLevel:
            item.structuralLevel,

          structuralZoneLow:
            item.structuralZoneLow,

          structuralZoneHigh:
            item.structuralZoneHigh,

          timeframes:
            item.structuralTimeframes,

          message:
            "STRUCTURAL + RESTING LIQUIDITY CONFLUENCE"

        });

      }


      /* ------------------------------------------------------
         Strength change events
      ------------------------------------------------------ */

      const enoughForStrengthEvent =
        item.persistent &&
        previousUsd > 0 &&
        item.usd >=
          SETTINGS.eventMinPersistentUsd;


      const changePct =
        previousUsd > 0
          ? Math.abs(
              item.changeUsd
            ) /
            previousUsd
          : 0;


      if (
        enoughForStrengthEvent &&
        changePct >=
          SETTINGS.eventStrengthChangePct &&
        now() -
          item.strengthEventAt >
          60000
      ) {

        if (
          item.changeUsd > 0
        ) {

          pushLiquidityEvent({

            key:
              `STRENGTHENING|${matchId}|${Math.floor(now() / 60000)}`,

            type:
              "STRENGTHENING",

            time:
              now(),

            side:
              eventSide(side),

            bookPriceLow:
              item.priceLow,

            bookPriceHigh:
              item.priceHigh,

            currentUsd:
              item.usd,

            maxUsd:
              item.maxUsd,

            durationMs:
              item.durationMs,

            lifetimeSec:
              item.durationSec,

            observations:
              item.observations,

            structuralLevel:
              item.structuralLevel,

            structuralZoneLow:
              item.structuralZoneLow,

            structuralZoneHigh:
              item.structuralZoneHigh,

            timeframes:
              item.structuralTimeframes,

            message:
              "LIQUIDITY STRENGTHENING"

          });

        } else {

          pushLiquidityEvent({

            key:
              `WEAKENING|${matchId}|${Math.floor(now() / 60000)}`,

            type:
              "WEAKENING",

            time:
              now(),

            side:
              eventSide(side),

            bookPriceLow:
              item.priceLow,

            bookPriceHigh:
              item.priceHigh,

            currentUsd:
              item.usd,

            maxUsd:
              item.maxUsd,

            durationMs:
              item.durationMs,

            lifetimeSec:
              item.durationSec,

            observations:
              item.observations,

            structuralLevel:
              item.structuralLevel,

            structuralZoneLow:
              item.structuralZoneLow,

            structuralZoneHigh:
              item.structuralZoneHigh,

            timeframes:
              item.structuralTimeframes,

            message:
              "LIQUIDITY WEAKENING"

          });

        }


        item.strengthEventAt =
          now();

      }

    } else {

      /* ------------------------------------------------------
         NEW CLUSTER
      ------------------------------------------------------ */

      const id =
        `${side}-${Math.round(
          cluster.midpoint
        )}-${now()}`;


      const item =
        createPersistenceItem(
          cluster
        );


      map.set(
        id,
        item
      );


      matchedIds.add(
        id
      );

    }

  }


  /* ----------------------------------------------------------
     DETECT REMOVED CLUSTERS
  ---------------------------------------------------------- */

  for (
    const [
      id,
      item
    ] of map.entries()
  ) {

    if (
      matchedIds.has(id) ||
      item.removed
    ) {

      continue;

    }


    const lifetime =
      now() -
      new Date(
        item.firstSeen
      ).getTime();


    const meaningfulRemoval =
      item.observations >=
        SETTINGS.persistenceMinObservationsForRemoval ||
      lifetime >=
        SETTINGS.persistenceMinLifetimeMs;


    item.removed = true;

    item.persistent = false;

    item.currentUsd = 0;

    item.usd = 0;

    item.previousUsd =
      item.previousUsd ||
      item.maxUsd;


    item.changeUsd =
      -item.previousUsd;


    item.durationMs =
      lifetime;


    item.durationSec =
      Math.floor(
        lifetime / 1000
      );


    item.status =
      "REMOVED";


    item.lastSeen =
      new Date(
        now()
      ).toISOString();


    /* --------------------------------------------------------
       Only meaningful removals become events.
    -------------------------------------------------------- */

    if (
      meaningfulRemoval &&
      !item.removedEventSent &&
      item.maxUsd >=
        SETTINGS.eventMinPersistentUsd
    ) {

      item.removedEventSent =
        true;


      pushLiquidityEvent({

        key:
          `REMOVED|${id}|${now()}`,

        type:
          "REMOVED",

        time:
          now(),

        side:
          eventSide(side),

        bookPriceLow:
          item.priceLow,

        bookPriceHigh:
          item.priceHigh,

        currentUsd: 0,

        maxUsd:
          item.maxUsd,

        durationMs:
          item.durationMs,

        lifetimeSec:
          item.durationSec,

        observations:
          item.observations,

        structuralLevel:
          item.structuralLevel,

        structuralZoneLow:
          item.structuralZoneLow,

        structuralZoneHigh:
          item.structuralZoneHigh,

        timeframes:
          item.structuralTimeframes,

        message:
          "LIQUIDITY REMOVED"

      });

    }

  }


  /* ----------------------------------------------------------
     Remove very old persistence records.
  ---------------------------------------------------------- */

  for (
    const [
      id,
      item
    ] of map.entries()
  ) {

    const age =
      now() -
      new Date(
        item.lastSeen
      ).getTime();


    if (
      item.removed &&
      age >
        SETTINGS.persistenceMaxAgeMs
    ) {

      map.delete(
        id
      );

    }

  }

}


/* ============================================================
   UPDATE PERSISTENCE
============================================================ */

function updatePersistence() {

  const book =
    getBook();


  const enrichedBids =
    enrichBookClusters(
      book.bids
    );


  const enrichedAsks =
    enrichBookClusters(
      book.asks
    );


  updatePersistenceSide(
    "bid",
    enrichedBids
  );


  updatePersistenceSide(
    "ask",
    enrichedAsks
  );


  persistence.lastPollAt =
    new Date().toISOString();

}


/* ============================================================
   PERSISTENT LIQUIDITY OUTPUT
============================================================ */

function getPersistentLiquidity() {

  const collect =
    map =>
      [...map.values()]
        .filter(
          item =>
            !item.removed ||
            item.maxUsd >=
              SETTINGS.eventMinPersistentUsd
        )
        .sort(
          (a, b) =>
            b.maxUsd -
            a.maxUsd
        );


  return {

    bids:
      collect(
        persistence.bids
      ),

    asks:
      collect(
        persistence.asks
      )

  };

}


/* ============================================================
   SWEEP DETECTION
============================================================ */

function detectCandleSweeps(
  tf,
  candle
) {

  if (
    !candle ||
    !candle.close
  ) {

    return;

  }


  const cutoff =
    now() -
    SETTINGS.sweepLookbackMs;


  for (
    const level of structure.levels
  ) {

    if (
      !isMajorLevel(level)
    ) {

      continue;

    }


    if (
      level.side === "BSL"
    ) {

      if (
        candle.high <=
        level.priceHigh
      ) {

        continue;

      }


      const penetrationPct =
        (
          candle.high -
          level.priceHigh
        ) /
        level.priceHigh;


      if (
        penetrationPct <
        SETTINGS.sweepMinPenetrationPct
      ) {

        continue;

      }


      if (
        candle.close >=
        level.priceHigh
      ) {

        continue;

      }


      if (
        candle.openTime <
        cutoff
      ) {

        continue;

      }


      addSweep({

        side: "BSL",

        level:
          level.price,

        zoneLow:
          level.priceLow,

        zoneHigh:
          level.priceHigh,

        sweepPrice:
          candle.high,

        timeframe: tf,

        candleTime:
          candle.openTime,

        status:
          "SWEPT",

        reclaimed:
          true,

        detectedAt:
          now(),

        timeframes:
          level.timeframes

      });

    }


    if (
      level.side === "SSL"
    ) {

      if (
        candle.low >=
        level.priceLow
      ) {

        continue;

      }


      const penetrationPct =
        (
          level.priceLow -
          candle.low
        ) /
        level.priceLow;


      if (
        penetrationPct <
        SETTINGS.sweepMinPenetrationPct
      ) {

        continue;

      }


      if (
        candle.close <=
        level.priceLow
      ) {

        continue;

      }


      if (
        candle.openTime <
        cutoff
      ) {

        continue;

      }


      addSweep({

        side: "SSL",

        level:
          level.price,

        zoneLow:
          level.priceLow,

        zoneHigh:
          level.priceHigh,

        sweepPrice:
          candle.low,

        timeframe: tf,

        candleTime:
          candle.openTime,

        status:
          "SWEPT",

        reclaimed:
          true,

        detectedAt:
          now(),

        timeframes:
          level.timeframes

      });

    }

  }

}


/* ============================================================
   ADD SWEEP
============================================================ */

function addSweep(sweep) {

  const key =
    `${sweep.side}|${Math.round(
      sweep.level
    )}|${sweep.candleTime}`;


  const existing =
    sweepEvents.find(
      event =>
        event.key === key
    );


  if (existing) {

    if (
      !existing.timeframes.includes(
        sweep.timeframe
      )
    ) {

      existing.timeframes.push(
        sweep.timeframe
      );

    }

    return;

  }


  const item = {

    key,

    ...sweep

  };


  sweepEvents.unshift(
    item
  );


  while (
    sweepEvents.length >
    SETTINGS.eventMax
  ) {

    sweepEvents.pop();

  }


  pushLiquidityEvent({

    key:
      `SWEEP|${key}`,

    type:
      "SWEEP",

    time:
      now(),

    side:
      sweep.side,

    bookPriceLow:
      sweep.zoneLow,

    bookPriceHigh:
      sweep.zoneHigh,

    currentUsd: 0,

    maxUsd: 0,

    durationMs: 0,

    lifetimeSec: 0,

    observations: 1,

    structuralLevel:
      sweep.level,

    structuralZoneLow:
      sweep.zoneLow,

    structuralZoneHigh:
      sweep.zoneHigh,

    timeframes:
      sweep.timeframes,

    message:
      `${sweep.side} LIQUIDITY SWEPT AND RECLAIMED`

  });

}


/* ============================================================
   CANDLE UPDATE
============================================================ */

function upsertCandle(
  tf,
  candle
) {

  if (
    !candles[tf]
  ) {

    candles[tf] = [];

  }


  const data =
    candles[tf];


  const existingIndex =
    data.findIndex(
      item =>
        item.openTime ===
        candle.openTime
    );


  if (
    existingIndex >= 0
  ) {

    data[
      existingIndex
    ] = candle;

  } else {

    data.push(
      candle
    );

  }


  while (
    data.length >
    STRUCTURE[tf].candles
  ) {

    data.shift();

  }

}


/* ============================================================
   PROCESS CLOSED CANDLE
============================================================ */

function processClosedCandle(
  tf,
  candle
) {

  /* ----------------------------------------------------------
     IMPORTANT:
     Detect sweep BEFORE rebuilding structure.
     This means the sweep is checked against the previously
     accepted/stable structure.
  ---------------------------------------------------------- */

  detectCandleSweeps(
    tf,
    candle
  );


  upsertCandle(
    tf,
    candle
  );


  rebuildStructure(
    `closed_${tf}`
  );

}


/* ============================================================
   KLINE SOCKET
============================================================ */

function connectKlineSocket() {

  if (
    klineSocket
  ) {

    try {
      klineSocket.close();
    } catch {}

  }


  klineSocket =
    new WebSocket(
      KLINE_WS
    );


  klineSocket.on(
    "open",
    () => {

      console.log(
        "[KLINE] connected"
      );

    }
  );


  klineSocket.on(
    "message",
    raw => {

      try {

        const packet =
          JSON.parse(
            raw.toString()
          );


        const payload =
          packet.data;


        if (
          !payload ||
          payload.e !==
            "kline"
        ) {

          return;

        }


        const k =
          payload.k;


        const tf =
          k.i;


        if (
          !STRUCTURE[tf]
        ) {

          return;

        }


        const candle = {

          openTime:
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
            Number(k.T)

        };


        upsertCandle(
          tf,
          candle
        );


        if (
          k.x
        ) {

          processClosedCandle(
            tf,
            candle
          );

        }

      } catch (error) {

        state.lastError =
          `Kline parse: ${error.message}`;

      }

    }
  );


  klineSocket.on(
    "close",
    () => {

      console.log(
        "[KLINE] disconnected"
      );


      setTimeout(
        connectKlineSocket,
        3000
      );

    }
  );


  klineSocket.on(
    "error",
    error => {

      state.lastError =
        `Kline WS: ${error.message}`;

      console.error(
        "[KLINE]",
        error.message
      );

    }
  );

}


/* ============================================================
   SNAPSHOT REQUEST
============================================================ */

function requestDepthSnapshot() {

  if (
    state.depthSnapshotPending
  ) {

    return;

  }


  state.depthSnapshotPending =
    true;


  state.depthSnapshotRequests++;


  const requestId =
    `depth-${now()}-${Math.random()
      .toString(16)
      .slice(2)}`;


  const ws =
    new WebSocket(
      WS_API
    );


  ws.on(
    "open",
    () => {

      const request = {

        id: requestId,

        method: "depth",

        params: {

          symbol: SYMBOL,

          limit: 1000

        }

      };


      ws.send(
        JSON.stringify(
          request
        )
      );

    }
  );


  ws.on(
    "message",
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );


        if (
          data.status ===
          429
        ) {

          state.depthSnapshot429s++;

          throw new Error(
            "Snapshot HTTP 429"
          );

        }


        if (
          data.status !==
          200 ||
          !data.result
        ) {

          throw new Error(
            `Snapshot failed: ${
              JSON.stringify(
                data
              )
            }`
          );

        }


        applySnapshot(
          data.result
        );


        state.depthSnapshotPending =
          false;


        state.lastSnapshotAt =
          new Date().toISOString();


        try {
          ws.close();
        } catch {}

      } catch (error) {

        state.depthSnapshotPending =
          false;

        state.lastError =
          error.message;

        try {
          ws.close();
        } catch {}

      }

    }
  );


  ws.on(
    "error",
    error => {

      state.depthSnapshotPending =
        false;

      state.lastError =
        `Snapshot WS: ${error.message}`;

      try {
        ws.close();
      } catch {}

    }
  );

}


/* ============================================================
   APPLY SNAPSHOT
============================================================ */

function applySnapshot(
  snapshot
) {

  const snapshotId =
    Number(
      snapshot.lastUpdateId
    );


  if (
    !Number.isFinite(
      snapshotId
    )
  ) {

    throw new Error(
      "Invalid snapshot update ID"
    );

  }


  const newBids =
    new Map();


  const newAsks =
    new Map();


  for (
    const row of
    snapshot.bids || []
  ) {

    const price =
      Number(row[0]);

    const qty =
      Number(row[1]);


    if (
      qty > 0
    ) {

      newBids.set(
        price,
        qty
      );

    }

  }


  for (
    const row of
    snapshot.asks || []
  ) {

    const price =
      Number(row[0]);

    const qty =
      Number(row[1]);


    if (
      qty > 0
    ) {

      newAsks.set(
        price,
        qty
      );

    }

  }


  state.bids =
    newBids;

  state.asks =
    newAsks;


  state.depthLastUpdateId =
    snapshotId;


  state.depthInitialized =
    false;


  state.depthBridgeAttempts++;


  /* ----------------------------------------------------------
     Find the event that bridges snapshot to stream.
  ---------------------------------------------------------- */

  const required =
    snapshotId + 1;


  let bridgeIndex = -1;


  for (
    let i = 0;
    i <
      state.depthPendingEvents.length;
    i++
  ) {

    const event =
      state.depthPendingEvents[i];


    if (
      event.U <= required &&
      event.u >= required
    ) {

      bridgeIndex = i;

      break;

    }

  }


  if (
    bridgeIndex < 0
  ) {

    if (
      state.depthPendingEvents.length
    ) {

      const latest =
        state.depthPendingEvents[
          state.depthPendingEvents.length - 1
        ];


      if (
        latest.u <
        required
      ) {

        state.depthSnapshotPending =
          false;

        setTimeout(
          requestDepthSnapshot,
          250
        );

        return;

      }

    }


    state.depthSnapshotPending =
      false;

    return;

  }


  state.depthBridgeFound++;


  for (
    let i = bridgeIndex;
    i <
      state.depthPendingEvents.length;
    i++
  ) {

    applyDepthEvent(
      state.depthPendingEvents[i],
      true
    );

  }


  state.depthPendingEvents =
    [];


  state.depthInitialized =
    true;


  state.depthSnapshotPending =
    false;


  updatePriceFromBook();

}


/* ============================================================
   APPLY DEPTH EVENT
============================================================ */

function applyDepthEvent(
  event,
  bridging = false
) {

  if (
    !state.depthInitialized &&
    !bridging
  ) {

    state.depthPendingEvents.push(
      event
    );

    return;

  }


  const previous =
    state.depthLastUpdateId;


  if (
    !bridging &&
    previous &&
    event.U >
      previous + 1
  ) {

    state.depthSequenceGaps++;

    state.depthResyncs++;

    state.depthInitialized =
      false;


    state.depthPendingEvents =
      [];


    requestDepthSnapshot();

    return;

  }


  for (
    const row of
    event.b || []
  ) {

    const price =
      Number(row[0]);

    const qty =
      Number(row[1]);


    if (
      qty === 0
    ) {

      state.bids.delete(
        price
      );

    } else {

      state.bids.set(
        price,
        qty
      );

    }

  }


  for (
    const row of
    event.a || []
  ) {

    const price =
      Number(row[0]);

    const qty =
      Number(row[1]);


    if (
      qty === 0
    ) {

      state.asks.delete(
        price
      );

    } else {

      state.asks.set(
        price,
        qty
      );

    }

  }


  state.depthLastUpdateId =
    event.u;


  state.lastDepthEventAt =
    new Date().toISOString();


  state.lastBookUpdateAt =
    state.lastDepthEventAt;


  updatePriceFromBook();

}


/* ============================================================
   PRICE FROM BOOK
============================================================ */

function updatePriceFromBook() {

  if (
    !state.bids.size ||
    !state.asks.size
  ) {

    return;

  }


  let bestBid = 0;

  let bestAsk = Infinity;


  for (
    const price of
    state.bids.keys()
  ) {

    if (
      price >
      bestBid
    ) {

      bestBid =
        price;

    }

  }


  for (
    const price of
    state.asks.keys()
  ) {

    if (
      price <
      bestAsk
    ) {

      bestAsk =
        price;

    }

  }


  if (
    bestBid > 0 &&
    Number.isFinite(bestAsk)
  ) {

    state.currentPrice =
      (
        bestBid +
        bestAsk
      ) / 2;

    state.priceSource =
      "futures_orderbook_mid";

  }

}


/* ============================================================
   DEPTH SOCKET
============================================================ */

function connectDepthSocket() {

  if (
    depthSocket
  ) {

    try {
      depthSocket.close();
    } catch {}

  }


  depthSocket =
    new WebSocket(
      DEPTH_WS
    );


  depthSocket.on(
    "open",
    () => {

      console.log(
        "[DEPTH] connected"
      );


      state.depthConnected =
        true;


      state.depthInitialized =
        false;


      state.depthPendingEvents =
        [];


      requestDepthSnapshot();

    }
  );


  depthSocket.on(
    "message",
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );


        if (
          !data ||
          !data.e
        ) {

          return;

        }


        const event = {

          U:
            Number(data.U),

          u:
            Number(data.u),

          b:
            data.b || [],

          a:
            data.a || []

        };


        if (
          !state.depthInitialized
        ) {

          state.depthPendingEvents.push(
            event
          );


          if (
            state.depthPendingEvents.length >
            5000
          ) {

            state.depthPendingEvents =
              state.depthPendingEvents.slice(
                -2500
              );

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

    }
  );


  depthSocket.on(
    "close",
    () => {

      console.log(
        "[DEPTH] disconnected"
      );


      state.depthConnected =
        false;

      state.depthInitialized =
        false;


      if (
        depthReconnectTimer
      ) {

        clearTimeout(
          depthReconnectTimer
        );

      }


      depthReconnectTimer =
        setTimeout(
          connectDepthSocket,
          3000
        );

    }
  );


  depthSocket.on(
    "error",
    error => {

      state.lastError =
        `Depth WS: ${error.message}`;

      console.error(
        "[DEPTH]",
        error.message
      );

    }
  );

}


/* ============================================================
   INTELLIGENCE STATE
============================================================ */

function intelligenceState(
  majorBSL,
  majorSSL
) {

  if (
    !majorBSL.length &&
    !majorSSL.length
  ) {

    return "NO MAJOR STRUCTURAL LIQUIDITY";

  }


  if (
    majorBSL.length &&
    majorSSL.length
  ) {

    return "BETWEEN MAJOR LIQUIDITY";

  }


  if (
    majorBSL.length
  ) {

    return "BELOW MAJOR BUY-SIDE LIQUIDITY";

  }


  return "ABOVE MAJOR SELL-SIDE LIQUIDITY";

}


/* ============================================================
   FORMAT STRUCTURAL LEVEL
============================================================ */

function formatStructuralLevel(
  level
) {

  if (!level) {
    return null;
  }


  const distancePct =
    state.currentPrice
      ? (
          (
            level.price -
            state.currentPrice
          ) /
          state.currentPrice
        ) *
        100
      : null;


  return {

    side:
      level.side,

    price:
      round(
        level.price,
        2
      ),

    priceLow:
      round(
        level.priceLow,
        2
      ),

    priceHigh:
      round(
        level.priceHigh,
        2
      ),

    score:
      round(
        level.score,
        2
      ),

    scoreBase:
      round(
        level.scoreBase,
        2
      ),

    strength:
      level.strength,

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      round(
        level.restingUsd,
        2
      ),

    restingConfirmed:
      level.restingConfirmed,

    restingLevels:
      level.restingLevels,

    distancePct:
      round(
        distancePct,
        3
      ),

    timeframeWeight:
      timeframeWeight(
        level
      )

  };

}


/* ============================================================
   STRUCTURE STATUS
============================================================ */

function getStructureStatus() {

  return {

    activeLevels:
      structure.levels.length,

    activeMajorBSL:
      structural("BSL").length,

    activeMajorSSL:
      structural("SSL").length,

    lastRebuildAt:
      structure.lastRebuildAt,

    lastGoodAt:
      structure.lastGoodAt,

    lastRebuildAccepted:
      structure.lastRebuildAccepted,

    lastRebuildReason:
      structure.lastRebuildReason,

    rebuildCount:
      structure.rebuildCount,

    rejectedRebuilds:
      structure.rejectedRebuilds,

    lastCandidateLevels:
      structure.lastCandidateLevels,

    lastCandidateMajor:
      structure.lastCandidateMajor,

    retainedPrevious:
      structure.retainedPrevious,

    status:
      structure.status

  };

}


/* ============================================================
   INTELLIGENCE
============================================================ */

function getIntelligence() {

  const majorBSL =
    structural("BSL");


  const majorSSL =
    structural("SSL");


  const nearBSL =
    nearStructural("BSL");


  const nearSSL =
    nearStructural("SSL");


  const nextBSL =
    majorBSL
      .sort(
        (a, b) =>
          a.price -
          b.price
      );


  const nextSSL =
    majorSSL
      .sort(
        (a, b) =>
          b.price -
          a.price
      );


  const book =
    getBook();


  const enrichedBids =
    enrichBookClusters(
      book.bids
    );


  const enrichedAsks =
    enrichBookClusters(
      book.asks
    );


  const nearestBSL =
    majorBSL
      .filter(
        level =>
          level.price >
          state.currentPrice
      )
      .sort(
        (a, b) =>
          a.price -
          b.price
      )[0] ||
    null;


  const nearestSSL =
    majorSSL
      .filter(
        level =>
          level.price <
          state.currentPrice
      )
      .sort(
        (a, b) =>
          b.price -
          a.price
      )[0] ||
    null;


  return {

    state:
      intelligenceState(
        majorBSL,
        majorSSL
      ),

    structureStatus:
      getStructureStatus(),


    nearestBSL:
      formatStructuralLevel(
        nearestBSL
      ),

    nearestSSL:
      formatStructuralLevel(
        nearestSSL
      ),


    nextBSL:
      nextBSL
        .slice(
          0,
          5
        )
        .map(
          formatStructuralLevel
        ),

    nextSSL:
      nextSSL
        .slice(
          0,
          5
        )
        .map(
          formatStructuralLevel
        ),


    nearBSL:
      nearBSL
        .slice(
          0,
          5
        )
        .map(
          formatStructuralLevel
        ),

    nearSSL:
      nearSSL
        .slice(
          0,
          5
        )
        .map(
          formatStructuralLevel
        ),


    orderBook: {

      bids:
        enrichedBids,

      asks:
        enrichedAsks

    },


    persistentLiquidity:
      getPersistentLiquidity(),


    liquidityEvents:
      liquidityEvents
        .slice(
          0,
          20
        ),

    recentSweeps:
      sweepEvents
        .slice(
          0,
          20
        )

  };

}


/* ============================================================
   HEALTH
============================================================ */

app.get(
  "/health",
  (req, res) => {

    res.json({

      ok: true,

      symbol: SYMBOL,

      status:
        state.depthInitialized
          ? "live"
          : state.depthConnected
            ? "syncing"
            : "disconnected",

      websocketConnected:
        state.depthConnected,

      depthConnected:
        state.depthConnected,

      snapshotConnected:
        true,

      initialized:
        state.depthInitialized,

      waitingForBridge:
        state.depthConnected &&
        !state.depthInitialized,

      snapshotPending:
        state.depthSnapshotPending,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      bidLevels:
        state.bids.size,

      askLevels:
        state.asks.size,

      lastUpdateId:
        state.depthLastUpdateId,

      pendingEvents:
        state.depthPendingEvents.length,

      snapshotRequests:
        state.depthSnapshotRequests,

      snapshot429s:
        state.depthSnapshot429s,

      bridgeAttempts:
        state.depthBridgeAttempts,

      bridgeFound:
        state.depthBridgeFound,

      resyncs:
        state.depthResyncs,

      sequenceGaps:
        state.depthSequenceGaps,

      structure:
        getStructureStatus(),

      updatedAt:
        new Date().toISOString(),

      lastError:
        state.lastError

    });

  }
);


/* ============================================================
   BOOK ENDPOINT
============================================================ */

app.get(
  "/book",
  (req, res) => {

    const book =
      getBook();


    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      bids:
        enrichBookClusters(
          book.bids
        ),

      asks:
        enrichBookClusters(
          book.asks
        ),

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

    const book =
      getBook();


    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      buyLiquidity:
        enrichBookClusters(
          book.bids
        ),

      sellLiquidity:
        enrichBookClusters(
          book.asks
        ),

      persistentLiquidity:
        getPersistentLiquidity(),

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

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      structureStatus:
        getStructureStatus(),

      buySideLiquidity:
        structural("BSL")
          .map(
            formatStructuralLevel
          ),

      sellSideLiquidity:
        structural("SSL")
          .map(
            formatStructuralLevel
          ),

      majorResistance:
        structural("BSL")
          .filter(
            level =>
              level.price >
              state.currentPrice
          )
          .map(
            formatStructuralLevel
          ),

      majorSupport:
        structural("SSL")
          .filter(
            level =>
              level.price <
              state.currentPrice
          )
          .map(
            formatStructuralLevel
          ),

      nearBSL:
        nearStructural("BSL")
          .map(
            formatStructuralLevel
          ),

      nearSSL:
        nearStructural("SSL")
          .map(
            formatStructuralLevel
          ),

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

    res.json({

      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      intelligence:
        getIntelligence(),

      updatedAt:
        new Date().toISOString()

    });

  }
);


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
        sweepEvents
          .slice(
            0,
            20
          ),

      liquidityEvents:
        liquidityEvents
          .slice(
            0,
            30
          ),

      persistentLiquidity:
        getPersistentLiquidity(),

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
        "Binance BTCUSDT Major Liquidity Relay",

      version:
        "10.0",

      symbol:
        SYMBOL,

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
   MAINTENANCE LOOP
============================================================ */

setInterval(
  () => {

    try {

      if (
        state.depthInitialized
      ) {

        updatePriceFromBook();

        updatePersistence();

      }


      /* ------------------------------------------------------
         Re-evaluate structure periodically.

         This also allows major status to update when price
         moves significantly without waiting for a new candle.
      ------------------------------------------------------ */

      rebuildStructure(
        "maintenance"
      );

    } catch (error) {

      state.lastError =
        `Maintenance: ${error.message}`;

      console.error(
        "[MAINTENANCE]",
        error.message
      );

    }

  },
  SETTINGS.persistencePollMs
);


/* ============================================================
   START
============================================================ */

async function start() {

  console.log(
    "================================================"
  );

  console.log(
    "BTCUSDT MAJOR LIQUIDITY RELAY v10.0"
  );

  console.log(
    "================================================"
  );


  try {

    await loadHistory();

  } catch (error) {

    state.lastError =
      `Startup history: ${error.message}`;

    console.error(
      "[STARTUP HISTORY]",
      error.message
    );

  }


  connectDepthSocket();

  connectKlineSocket();


  app.listen(
    PORT,
    () => {

      console.log(
        `[HTTP] listening on ${PORT}`
      );

    }
  );

}


start();


/* ============================================================
   PROCESS SAFETY
============================================================ */

process.on(
  "uncaughtException",
  error => {

    state.lastError =
      `uncaughtException: ${error.message}`;

    console.error(
      "[UNCAUGHT]",
      error
    );

  }
);


process.on(
  "unhandledRejection",
  error => {

    state.lastError =
      `unhandledRejection: ${
        error?.message ||
        String(error)
      }`;

    console.error(
      "[UNHANDLED]",
      error
    );

  }
);
