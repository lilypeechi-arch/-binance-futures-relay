const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const SYMBOL = 'BTCUSDT';

const SPOT_API = 'https://data-api.binance.vision';

const DEPTH_WS =
  'wss://fstream.binance.com/ws/btcusdt@depth@100ms';

const KLINE_WS =
  'wss://fstream.binance.com/stream?streams=btcusdt@kline_15m/btcusdt@kline_1h/btcusdt@kline_4h/btcusdt@kline_1d';

const SNAPSHOT_WS =
  'wss://ws-fapi.binance.com/ws-fapi/v1';

/* ============================================================
   STRUCTURE SETTINGS
============================================================ */

const STRUCTURE = {
  '15m': {
    interval: '15m',
    weight: 1,
    candles: 300,
    left: 3,
    right: 3,
    eq: 0.0015
  },

  '1h': {
    interval: '1h',
    weight: 2,
    candles: 300,
    left: 4,
    right: 4,
    eq: 0.0015
  },

  '4h': {
    interval: '4h',
    weight: 3,
    candles: 250,
    left: 5,
    right: 5,
    eq: 0.0020
  },

  '1d': {
    interval: '1d',
    weight: 4,
    candles: 180,
    left: 5,
    right: 5,
    eq: 0.0025
  }
};

/* ============================================================
   SETTINGS
============================================================ */

const SETTINGS = {

  /* Raw order book */
  rawMinUsd: 50000,

  /* Structural resting liquidity */
  restingRadiusUsd: 35,
  restingConfirmUsd: 250000,

  /* Structure merging */
  mergeUsd: 35,
  zonePct: 0.0015,
  reactionPct: 0.003,

  /* Structure */
  minScore: 40,
  maxLevels: 12,

  /*
     ----------------------------------------------------------
     ORDER-BOOK CONCENTRATION
     ----------------------------------------------------------

     IMPORTANT:

     The previous version chained neighboring order-book
     levels together. BTC has many normal levels closer than
     $35 apart, so that created fake $20M-$30M "walls".

     We now:
       1. bucket prices
       2. calculate USD per bucket
       3. keep only strong buckets
       4. join only nearby strong buckets
       5. enforce maximum zone width
  */

  bookBucketUsd: 10,

  bookBucketMinUsd: 250000,

  orderMinUsd: 500000,

  minStrongBuckets: 2,

  /*
     A single exceptionally large price bucket can qualify
     without needing a second bucket.
  */
  singleBucketMinUsd: 1000000,

  /*
     Strong buckets can have a small gap between them.
  */
  clusterGapUsd: 25,

  /*
     Never allow one concentration zone to become huge.
  */
  maxClusterWidthUsd: 200,

  /*
     Number of raw book levels represented by a cluster
     is diagnostic only.
  */
  minClusterLevels: 2,

  /* Persistence */
  persistencePollMs: 5000,
  persistenceConfirmMs: 60000,
  persistenceMaxAgeMs: 10 * 60 * 1000,

  /*
     Match moving concentration zones.
  */
  persistenceTolerancePct: 0.0007,

  /* Ignore one-snapshot noise */
  minimumRemovedObservations: 2,
  minimumRemovedLifetimeMs: 10000,

  /* Events */
  eventMax: 100,

  /* Sweeps */
  sweepLookbackMs: 24 * 60 * 60 * 1000,

  sweepMinPenetrationPct: 0.00015,

  /* Major liquidity filtering */
  majorMinDistancePct: 0.50,
  majorMinimumTimeframeWeight: 2,
  major1hMinScore: 90,
  majorHigherTfMinScore: 40,

  /*
     Structure stability.

     A rebuild which temporarily produces zero major levels
     must not erase the last valid structural map.
  */
  structureKeepAliveMs: 30 * 60 * 1000,

  /*
     At least this many total structural levels are preferred
     before replacing the active map.
  */
  structureMinimumLevels: 2,

  /*
     Confluence between order-book concentration and structural
     liquidity should be relatively tight.
  */
  confluenceMaxPct: 0.0025,
  confluenceMaxUsd: 150
};

/* ============================================================
   CANDLE DATA
============================================================ */

const candles = {
  '15m': [],
  '1h': [],
  '4h': [],
  '1d': []
};

/* ============================================================
   ORDER BOOK
============================================================ */

const book = {
  bids: new Map(),
  asks: new Map()
};

/* ============================================================
   STRUCTURE
============================================================ */

const structure = {

  swingHighs: [],
  swingLows: [],

  equalHighs: [],
  equalLows: [],

  levels: [],

  /*
     Stable structural map.
  */
  lastGoodLevels: [],

  lastGoodAt: null,

  lastRebuildAt: null,

  lastRebuildAccepted: false,

  lastRebuildReason: null,

  rebuildCount: 0
};

/* ============================================================
   PERSISTENCE
============================================================ */

const persistence = {
  bids: [],
  asks: []
};

/* ============================================================
   STATE
============================================================ */

const state = {

  currentPrice: null,
  priceSource: null,

  depthConnected: false,
  snapshotConnected: false,
  klineConnected: false,

  bookInitialized: false,

  lastUpdateId: 0,

  pendingEvents: [],

  gapDetected: false,
  gapRecovery: false,
  gapCount: 0,

  lastGapExpected: null,
  lastGapReceived: null,

  lastDepthMessage: null,
  lastKlineMessage: null,

  updatedAt: null,

  snapshot: {

    pending: false,

    lastRequestAt: 0,

    lastSnapshotId: 0,

    bridgeAttempts: 0,

    bridgeFound: 0,

    snapshotRequests: 0,

    snapshot429s: 0,

    lastError: null
  },

  sweeps: [],

  liquidityEvents: [],

  candleStats: {

    '15m': {
      loaded: 0,
      lastOpenTime: null
    },

    '1h': {
      loaded: 0,
      lastOpenTime: null
    },

    '4h': {
      loaded: 0,
      lastOpenTime: null
    },

    '1d': {
      loaded: 0,
      lastOpenTime: null
    }
  }
};

/* ============================================================
   HELPERS
============================================================ */

function now() {
  return Date.now();
}

function round(n, d = 2) {
  return Number(Number(n).toFixed(d));
}

function pctDistance(price, current) {

  if (!current) {
    return null;
  }

  return round(
    ((price - current) / current) * 100,
    3
  );
}

function intervalMs(tf) {

  return (
    {
      '15m': 15,
      '1h': 60,
      '4h': 240,
      '1d': 1440
    }[tf] || 15
  ) * 60 * 1000;
}

function safeDate(ms) {

  if (!Number.isFinite(ms)) {
    return null;
  }

  return new Date(ms).toISOString();
}

/* ============================================================
   PRICE
============================================================ */

function updatePrice(price, source) {

  const p = Number(price);

  if (
    !Number.isFinite(p) ||
    p <= 0
  ) {
    return;
  }

  state.currentPrice = p;
  state.priceSource = source;
  state.updatedAt =
    new Date().toISOString();
}

function updatePriceFromBook() {

  if (
    !book.bids.size ||
    !book.asks.size
  ) {
    return;
  }

  const bids =
    [...book.bids.keys()]
      .sort((a, b) => b - a);

  const asks =
    [...book.asks.keys()]
      .sort((a, b) => a - b);

  if (
    !bids.length ||
    !asks.length
  ) {
    return;
  }

  const bestBid = bids[0];
  const bestAsk = asks[0];

  updatePrice(
    (bestBid + bestAsk) / 2,
    'futures_orderbook_mid'
  );
}

/* ============================================================
   CANDLES
============================================================ */

function candleFromKline(k) {

  return {

    openTime: Number(k.t),
    closeTime: Number(k.T),

    open: Number(k.o),
    high: Number(k.h),
    low: Number(k.l),
    close: Number(k.c),

    volume: Number(k.v)
  };
}

/* ============================================================
   HISTORICAL SPOT
============================================================ */

async function fetchSpotKlines(
  interval,
  limit
) {

  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${limit}`;

  const res =
    await fetch(url);

  if (!res.ok) {

    throw new Error(
      `Spot klines ${interval}: HTTP ${res.status}`
    );
  }

  const rows =
    await res.json();

  return rows.map(r => ({

    openTime: Number(r[0]),
    closeTime: Number(r[6]),

    open: Number(r[1]),
    high: Number(r[2]),
    low: Number(r[3]),
    close: Number(r[4]),

    volume: Number(r[5])
  }));
}

async function loadHistory() {

  for (
    const [tf, cfg]
    of Object.entries(STRUCTURE)
  ) {

    try {

      candles[tf] =
        await fetchSpotKlines(
          cfg.interval,
          cfg.candles
        );

      state.candleStats[tf].loaded =
        candles[tf].length;

      state.candleStats[tf].lastOpenTime =
        candles[tf].at(-1)?.openTime || null;

      console.log(
        `Loaded ${tf}: ${candles[tf].length} candles`
      );

    } catch (err) {

      console.error(
        `History ${tf} failed:`,
        err.message
      );
    }
  }

  rebuildStructure('initial_history');
}

/* ============================================================
   PIVOTS
============================================================ */

function isPivotHigh(
  arr,
  i,
  left,
  right
) {

  const p = arr[i].high;

  for (
    let j = i - left;
    j <= i + right;
    j++
  ) {

    if (j === i) {
      continue;
    }

    if (
      j < 0 ||
      j >= arr.length ||
      arr[j].high >= p
    ) {

      return false;
    }
  }

  return true;
}

function isPivotLow(
  arr,
  i,
  left,
  right
) {

  const p = arr[i].low;

  for (
    let j = i - left;
    j <= i + right;
    j++
  ) {

    if (j === i) {
      continue;
    }

    if (
      j < 0 ||
      j >= arr.length ||
      arr[j].low <= p
    ) {

      return false;
    }
  }

  return true;
}

/* ============================================================
   REACTION COUNT
============================================================ */

function reactionCount(
  arr,
  index,
  side,
  price
) {

  let count = 0;

  const start =
    Math.max(
      0,
      index - 8
    );

  let lastReaction = -Infinity;

  for (
    let i = start;
    i < Math.min(
      arr.length,
      index + 9
    );
    i++
  ) {

    if (i === index) {
      continue;
    }

    const c = arr[i];

    const tolerance =
      price *
      SETTINGS.reactionPct;

    const touched =
      side === 'BSL'
        ? c.high >=
          price - tolerance
        : c.low <=
          price + tolerance;

    if (!touched) {
      continue;
    }

    const reaction =
      side === 'BSL'
        ? (
            c.close < c.open &&
            c.close < price
          )
        : (
            c.close > c.open &&
            c.close > price
          );

    if (
      reaction &&
      i - lastReaction >= 2
    ) {

      count++;

      lastReaction = i;
    }
  }

  return count;
}

/* ============================================================
   TIMEFRAME STRUCTURE
============================================================ */

function timeframeStructure(
  tf,
  arr,
  cfg
) {

  const highs = [];
  const lows = [];

  if (!arr.length) {

    return {
      highs,
      lows
    };
  }

  for (
    let i = cfg.left;
    i < arr.length - cfg.right;
    i++
  ) {

    if (
      isPivotHigh(
        arr,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      highs.push({

        side: 'BSL',

        price:
          arr[i].high,

        time:
          arr[i].openTime,

        index:
          i,

        timeframe:
          tf,

        weight:
          cfg.weight,

        reactions:
          reactionCount(
            arr,
            i,
            'BSL',
            arr[i].high
          )
      });
    }

    if (
      isPivotLow(
        arr,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      lows.push({

        side: 'SSL',

        price:
          arr[i].low,

        time:
          arr[i].openTime,

        index:
          i,

        timeframe:
          tf,

        weight:
          cfg.weight,

        reactions:
          reactionCount(
            arr,
            i,
            'SSL',
            arr[i].low
          )
      });
    }
  }

  return {
    highs,
    lows
  };
}

/* ============================================================
   STRUCTURE LEVEL
============================================================ */

function buildLevel(group) {

  const tfs = [
    ...new Set(
      group.points.map(
        p => p.timeframe
      )
    )
  ];

  const weights =
    tfs.reduce(
      (s, tf) =>
        s +
        (STRUCTURE[tf]?.weight || 0),
      0
    );

  const equalCount =
    group.points.length;

  const reactions =
    group.points.reduce(
      (s, p) =>
        s + p.reactions,
      0
    );

  const prices =
    group.points.map(
      p => p.price
    );

  const zoneLow =
    Math.min(...prices) *
    (1 - SETTINGS.zonePct);

  const zoneHigh =
    Math.max(...prices) *
    (1 + SETTINGS.zonePct);

  const score =
    weights * 10 +
    Math.min(equalCount, 30) * 5 +
    Math.min(reactions, 40) * 2;

  const level = {

    side:
      group.side,

    price:
      group.points.reduce(
        (s, x) =>
          s + x.price,
        0
      ) /
      group.points.length,

    priceLow:
      zoneLow,

    priceHigh:
      zoneHigh,

    score,

    scoreBase:
      score,

    strength:
      'LOW',

    timeframes:
      tfs,

    equalCount,

    reactions,

    restingUsd:
      0,

    restingConfirmed:
      false,

    restingLevels:
      [],

    distancePct:
      null,

    timeframeWeight:
      weights
  };

  updateLevelStrength(level);

  return level;
}

/* ============================================================
   LEVEL STRENGTH
============================================================ */

function updateLevelStrength(level) {

  if (
    level.score >= 125 ||
    (
      level.timeframes.length >= 3 &&
      level.equalCount >= 4
    )
  ) {

    level.strength =
      'HIGH';

  } else if (
    level.score >= 90 ||
    level.timeframes.length >= 2 ||
    level.equalCount >= 3
  ) {

    level.strength =
      'MEDIUM';

  } else {

    level.strength =
      'LOW';
  }
}

/* ============================================================
   STRUCTURE CLUSTERING
============================================================ */

function mergeAcrossTimeframes(
  allPoints,
  side
) {

  const groups = [];

  const sorted =
    [...allPoints]
      .sort(
        (a, b) =>
          a.price - b.price
      );

  for (
    const p of sorted
  ) {

    let best = null;
    let bestDistance = Infinity;

    for (
      const g of groups
    ) {

      const avg =
        g.points.reduce(
          (s, x) =>
            s + x.price,
          0
        ) /
        g.points.length;

      const tolerance =
        Math.max(
          avg * 0.0025,
          SETTINGS.mergeUsd
        );

      const distance =
        Math.abs(
          p.price - avg
        );

      if (
        distance <= tolerance &&
        distance < bestDistance
      ) {

        best = g;
        bestDistance =
          distance;
      }
    }

    if (!best) {

      groups.push({
        side,
        points: [p]
      });

    } else {

      best.points.push(p);
    }
  }

  return groups
    .map(buildLevel)
    .sort(
      (a, b) =>
        b.score - a.score
    )
    .slice(
      0,
      SETTINGS.maxLevels
    );
}

/* ============================================================
   MAJOR LEVEL
============================================================ */

function timeframeWeight(level) {

  return level.timeframes.reduce(
    (s, tf) =>
      s +
      (STRUCTURE[tf]?.weight || 0),
    0
  );
}

function isMajorLevel(level) {

  if (!state.currentPrice) {
    return false;
  }

  const distance =
    Math.abs(
      (
        level.price -
        state.currentPrice
      ) /
      state.currentPrice
    ) * 100;

  if (
    distance <
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

  const hasHigher =
    level.timeframes.some(
      tf =>
        tf === '4h' ||
        tf === '1d'
    );

  const has1h =
    level.timeframes.includes(
      '1h'
    );

  if (hasHigher) {

    return (
      level.score >=
      SETTINGS.majorHigherTfMinScore
    );
  }

  if (has1h) {

    return (
      level.score >=
      SETTINGS.major1hMinScore
    );
  }

  return false;
}

function isNearLevel(level) {

  if (!state.currentPrice) {
    return false;
  }

  return (
    Math.abs(
      (
        level.price -
        state.currentPrice
      ) /
      state.currentPrice
    ) * 100
    <
    SETTINGS.majorMinDistancePct
  );
}

/* ============================================================
   STRUCTURE REBUILD
============================================================ */

function rebuildStructure(
  reason = 'unknown'
) {

  const rebuildStarted =
    now();

  const allHighs = [];
  const allLows = [];

  const newSwingHighs = [];
  const newSwingLows = [];

  for (
    const [tf, cfg]
    of Object.entries(STRUCTURE)
  ) {

    const s =
      timeframeStructure(
        tf,
        candles[tf],
        cfg
      );

    newSwingHighs.push(
      ...s.highs
    );

    newSwingLows.push(
      ...s.lows
    );

    allHighs.push(
      ...s.highs
    );

    allLows.push(
      ...s.lows
    );
  }

  const newEqualHighs =
    allHighs.filter(
      p =>
        p.reactions >= 1
    );

  const newEqualLows =
    allLows.filter(
      p =>
        p.reactions >= 1
    );

  const highs =
    mergeAcrossTimeframes(
      allHighs,
      'BSL'
    );

  const lows =
    mergeAcrossTimeframes(
      allLows,
      'SSL'
    );

  const newLevels = [
    ...highs,
    ...lows
  ];

  /*
     Determine major levels BEFORE deciding
     whether to replace the stable map.
  */

  const previousLevels =
    structure.levels;

  const previousMajorCount =
    previousLevels.filter(
      isMajorLevel
    ).length;

  const newMajorCount =
    newLevels.filter(
      isMajorLevel
    ).length;

  const validData =
    newSwingHighs.length > 0 &&
    newSwingLows.length > 0 &&
    newLevels.length >=
      SETTINGS.structureMinimumLevels;

  let accept = false;
  let rebuildReason = '';

  if (validData) {

    /*
       Normal valid rebuild.
    */

    if (
      newMajorCount > 0
    ) {

      accept = true;

      rebuildReason =
        'valid_major_structure';

    } else if (
      previousMajorCount === 0
    ) {

      /*
         There was no previous major
         map to protect.
      */

      accept = true;

      rebuildReason =
        'valid_no_major_structure';

    } else {

      /*
         New rebuild lost all major levels.
         Preserve the last good map temporarily.
      */

      const lastGoodAge =
        structure.lastGoodAt
          ? now() -
            structure.lastGoodAt
          : Infinity;

      if (
        lastGoodAge <=
        SETTINGS.structureKeepAliveMs
      ) {

        accept = false;

        rebuildReason =
          'preserved_last_good_structure';

      } else {

        accept = true;

        rebuildReason =
          'last_good_structure_expired';
      }
    }

  } else {

    accept = false;

    rebuildReason =
      'insufficient_structure_data';
  }

  /*
     Always retain the newly calculated
     raw swing statistics.

     The ACTIVE LEVEL MAP is handled separately.
  */

  structure.swingHighs =
    newSwingHighs;

  structure.swingLows =
    newSwingLows;

  structure.equalHighs =
    newEqualHighs;

  structure.equalLows =
    newEqualLows;

  if (accept) {

    structure.levels =
      newLevels;

    /*
       Save a deep-ish copy so later
       applyResting() mutations do not
       corrupt the cached structural map.
    */

    structure.lastGoodLevels =
      newLevels.map(
        level => ({
          ...level,

          timeframes:
            [...level.timeframes],

          restingLevels:
            [...(level.restingLevels || [])]
        })
      );

    structure.lastGoodAt =
      now();

    structure.lastRebuildAccepted =
      true;

  } else {

    /*
       Keep previous active structure.
    */

    if (
      structure.lastGoodLevels.length
    ) {

      structure.levels =
        structure.lastGoodLevels.map(
          level => ({
            ...level,

            timeframes:
              [...level.timeframes],

            restingLevels:
              [...(level.restingLevels || [])]
          })
        );
    }

    structure.lastRebuildAccepted =
      false;
  }

  structure.lastRebuildAt =
    now();

  structure.lastRebuildReason =
    rebuildReason;

  structure.rebuildCount++;

  /*
     Reapply current order-book liquidity
     to whichever map is active.
  */

  applyResting();

  const duration =
    now() - rebuildStarted;

  console.log(
    `[STRUCTURE] ${reason} | ` +
    `raw=${newLevels.length} ` +
    `newMajor=${newMajorCount} ` +
    `previousMajor=${previousMajorCount} ` +
    `accepted=${accept} ` +
    `reason=${rebuildReason} ` +
    `duration=${duration}ms`
  );
}

/* ============================================================
   RAW BOOK ROWS
============================================================ */

function bookRows(side) {

  const map =
    side === 'bid'
      ? book.bids
      : book.asks;

  return [
    ...map.entries()
  ]

    .map(
      ([price, qty]) => ({

        price,

        quantity:
          qty,

        usd:
          price * qty
      })
    )

    .filter(
      x =>
        x.usd >=
        SETTINGS.rawMinUsd
    )

    .sort(
      (a, b) =>
        side === 'bid'
          ? b.price - a.price
          : a.price - b.price
    );
}

/* ============================================================
   STRUCTURAL RESTING
============================================================ */

function restingNear(
  price,
  side
) {

  const rows =
    bookRows(
      side === 'BSL'
        ? 'ask'
        : 'bid'
    );

  const levels =
    rows.filter(
      x =>
        Math.abs(
          x.price - price
        ) <=
        SETTINGS.restingRadiusUsd
    );

  return {

    usd:
      levels.reduce(
        (s, x) =>
          s + x.usd,
        0
      ),

    levels
  };
}

function applyResting() {

  for (
    const level
    of structure.levels
  ) {

    const r =
      restingNear(
        level.price,
        level.side
      );

    level.restingUsd =
      round(
        r.usd,
        2
      );

    level.restingLevels =
      r.levels
        .slice(0, 20)
        .map(
          x => ({

            price:
              round(
                x.price,
                2
              ),

            quantity:
              round(
                x.quantity,
                6
              ),

            usd:
              round(
                x.usd,
                2
              )
          })
        );

    level.restingConfirmed =
      r.usd >=
      SETTINGS.restingConfirmUsd;

    level.scoreBase =
      Number.isFinite(
        level.scoreBase
      )
        ? level.scoreBase
        : level.score;

    /*
       Resting liquidity can enhance
       the score, but never redefine
       the underlying structure.
    */

    const bonus =
      Math.min(
        25,
        Math.floor(
          r.usd /
          SETTINGS.restingConfirmUsd
        ) * 10
      );

    level.score =
      level.scoreBase +
      bonus;

    updateLevelStrength(level);

    level.distancePct =
      pctDistance(
        level.price,
        state.currentPrice
      );
  }
}

/* ============================================================
   BOOK PRICE BUCKET
============================================================ */

function bucketPrice(
  price
) {

  const size =
    SETTINGS.bookBucketUsd;

  return (
    Math.round(
      price / size
    ) * size
  );
}

/* ============================================================
   ORDER BOOK CONCENTRATION
============================================================ */

function bookClusters(side) {

  const rows =
    bookRows(side);

  if (!rows.length) {
    return [];
  }

  /*
     ----------------------------------------------------------
     STEP 1
     Aggregate raw levels into fixed price buckets.
     ----------------------------------------------------------
  */

  const buckets =
    new Map();

  for (
    const row
    of rows
  ) {

    const bucket =
      bucketPrice(
        row.price
      );

    let item =
      buckets.get(bucket);

    if (!item) {

      item = {

        bucket,

        priceLow:
          Infinity,

        priceHigh:
          -Infinity,

        usd: 0,

        levels: 0,

        rows: []
      };

      buckets.set(
        bucket,
        item
      );
    }

    item.priceLow =
      Math.min(
        item.priceLow,
        row.price
      );

    item.priceHigh =
      Math.max(
        item.priceHigh,
        row.price
      );

    item.usd +=
      row.usd;

    item.levels++;

    item.rows.push(row);
  }

  /*
     ----------------------------------------------------------
     STEP 2
     Keep only concentrated buckets.

     This is the critical change.

     Normal BTC book density will no longer
     become a giant cluster.
     ----------------------------------------------------------
  */

  const strong =
    [...buckets.values()]
      .filter(
        b =>
          b.usd >=
          SETTINGS.bookBucketMinUsd
      )
      .sort(
        (a, b) =>
          a.bucket -
          b.bucket
      );

  if (!strong.length) {
    return [];
  }

  /*
     ----------------------------------------------------------
     STEP 3
     Group nearby strong buckets.

     We enforce max width so a chain of
     hundreds of normal levels cannot
     become one giant liquidity zone.
     ----------------------------------------------------------
  */

  const groups = [];

  let current = [];

  for (
    const bucket
    of strong
  ) {

    if (!current.length) {

      current = [bucket];
      continue;
    }

    const last =
      current.at(-1);

    const gap =
      Math.abs(
        bucket.bucket -
        last.bucket
      );

    const currentLow =
      current[0].bucket;

    const proposedHigh =
      bucket.bucket;

    const width =
      proposedHigh -
      currentLow;

    if (
      gap <=
        SETTINGS.clusterGapUsd &&
      width <=
        SETTINGS.maxClusterWidthUsd
    ) {

      current.push(bucket);

    } else {

      groups.push(
        current
      );

      current = [
        bucket
      ];
    }
  }

  if (current.length) {
    groups.push(current);
  }

  /*
     ----------------------------------------------------------
     STEP 4
     Convert groups into actual liquidity zones.
     ----------------------------------------------------------
  */

  const result = [];

  for (
    const group
    of groups
  ) {

    const usd =
      group.reduce(
        (s, b) =>
          s + b.usd,
        0
      );

    const levels =
      group.reduce(
        (s, b) =>
          s + b.levels,
        0
      );

    const strongestBucket =
      group.reduce(
        (a, b) =>
          a.usd >= b.usd
            ? a
            : b
      );

    /*
       A multi-bucket zone requires
       at least 2 strong buckets.

       A single exceptional bucket can
       qualify by itself.
    */

    const qualifies =
      group.length >=
        SETTINGS.minStrongBuckets ||
      (
        group.length === 1 &&
        group[0].usd >=
          SETTINGS.singleBucketMinUsd
      );

    if (!qualifies) {
      continue;
    }

    if (
      usd <
      SETTINGS.orderMinUsd
    ) {
      continue;
    }

    const priceLow =
      Math.min(
        ...group.map(
          b => b.priceLow
        )
      );

    const priceHigh =
      Math.max(
        ...group.map(
          b => b.priceHigh
        )
      );

    const weightedMid =
      group.reduce(
        (s, b) =>
          s +
          b.bucket *
          b.usd,
        0
      ) /
      usd;

    result.push({

      side,

      priceLow,

      priceHigh,

      midpoint:
        weightedMid,

      usd,

      levels,

      strongBuckets:
        group.length,

      strongestPrice:
        strongestBucket.bucket,

      strongestBucketUsd:
        strongestBucket.usd
    });
  }

  return result
    .sort(
      (a, b) =>
        b.usd -
        a.usd
    );
}

/* ============================================================
   PERSISTENCE MATCH
============================================================ */

function clusterStillNear(
  current,
  previous
) {

  const tolerance =
    Math.max(
      SETTINGS.bookBucketUsd * 2,

      (current.midpoint || 0) *
        SETTINGS.persistenceTolerancePct
    );

  const midpointClose =
    Math.abs(
      current.midpoint -
      previous.midpoint
    ) <=
    tolerance;

  const zonesOverlap =
    current.priceLow <=
      previous.priceHigh &&
    current.priceHigh >=
      previous.priceLow;

  return (
    midpointClose ||
    zonesOverlap
  );
}

/* ============================================================
   PERSISTENCE STATUS
============================================================ */

function persistenceState(
  previous,
  currentUsd
) {

  if (!previous) {
    return 'NEW';
  }

  const previousUsd =
    previous.currentUsd || 0;

  if (
    previousUsd > 0 &&
    currentUsd >
      previousUsd * 1.15
  ) {

    return 'STRENGTHENING';
  }

  if (
    previousUsd > 0 &&
    currentUsd <
      previousUsd * 0.70
  ) {

    return 'WEAKENING';
  }

  return 'HOLDING';
}

/* ============================================================
   STRUCTURAL RELATIONSHIP
============================================================ */

function structuralNearBookCluster(
  p
) {

  const structuralSide =
    p.side === 'ask'
      ? 'BSL'
      : 'SSL';

  const candidates =
    structure.levels.filter(
      level =>
        level.side ===
        structuralSide &&
        isMajorLevel(level)
    );

  let best = null;
  let bestDistance =
    Infinity;

  for (
    const level
    of candidates
  ) {

    /*
       True distance between two zones.

       If zones overlap, distance = 0.
    */

    let distance = 0;

    if (
      p.priceHigh <
      level.priceLow
    ) {

      distance =
        level.priceLow -
        p.priceHigh;

    } else if (
      p.priceLow >
      level.priceHigh
    ) {

      distance =
        p.priceLow -
        level.priceHigh;
    }

    const maxDistance =
      Math.min(
        SETTINGS.confluenceMaxUsd,
        level.price *
          SETTINGS.confluenceMaxPct
      );

    if (
      distance <=
        maxDistance &&
      distance <
        bestDistance
    ) {

      best =
        level;

      bestDistance =
        distance;
    }
  }

  return best;
}

/* ============================================================
   LIQUIDITY EVENT
============================================================ */

function correlateLiquidityEvent(
  persistenceItem,
  type
) {

  const level =
    structuralNearBookCluster(
      persistenceItem
    );

  /*
     Persistent events only become
     CONFLUENCE when they overlap or
     sit tightly beside a major
     structural zone.
  */

  if (
    !level &&
    type !== 'REMOVED'
  ) {

    return;
  }

  const key =
    `${type}|${persistenceItem.id}|${level?.side || persistenceItem.side}`;

  const duplicate =
    state.liquidityEvents.find(
      e =>
        e.key === key &&
        now() - e.time < 60000
    );

  if (duplicate) {
    return;
  }

  let message =
    'PERSISTENT LIQUIDITY';

  if (
    type === 'REMOVED'
  ) {

    message =
      'LIQUIDITY REMOVED';

  } else if (
    level
  ) {

    message =
      'CONFLUENCE';
  }

  state.liquidityEvents.unshift({

    key,

    type,

    time:
      now(),

    side:
      persistenceItem.side === 'ask'
        ? 'BSL'
        : 'SSL',

    bookPriceLow:
      round(
        persistenceItem.priceLow,
        2
      ),

    bookPriceHigh:
      round(
        persistenceItem.priceHigh,
        2
      ),

    currentUsd:
      round(
        persistenceItem.currentUsd,
        2
      ),

    maxUsd:
      round(
        persistenceItem.maxUsd,
        2
      ),

    durationMs:
      persistenceItem.durationMs,

    lifetimeSec:
      round(
        persistenceItem.durationMs /
          1000,
        0
      ),

    observations:
      persistenceItem.observations,

    structuralLevel:
      level
        ? round(
            level.price,
            2
          )
        : null,

    structuralZoneLow:
      level
        ? round(
            level.priceLow,
            2
          )
        : null,

    structuralZoneHigh:
      level
        ? round(
            level.priceHigh,
            2
          )
        : null,

    timeframes:
      level
        ? level.timeframes
        : [],

    message
  });

  state.liquidityEvents =
    state.liquidityEvents.slice(
      0,
      SETTINGS.eventMax
    );
}

/* ============================================================
   PERSISTENCE UPDATE
============================================================ */

function updatePersistence() {

  const timestamp =
    now();

  for (
    const side
    of ['bids', 'asks']
  ) {

    const bookSide =
      side === 'bids'
        ? 'bid'
        : 'ask';

    const currentClusters =
      bookClusters(
        bookSide
      );

    const existing =
      persistence[side];

    const used =
      new Set();

    /*
       Match current clusters.
    */

    for (
      const cluster
      of currentClusters
    ) {

      let matchIndex = -1;
      let bestDistance =
        Infinity;

      for (
        let i = 0;
        i < existing.length;
        i++
      ) {

        if (
          used.has(i)
        ) {
          continue;
        }

        const previous =
          existing[i];

        if (
          previous.removed
        ) {
          continue;
        }

        if (
          !clusterStillNear(
            cluster,
            previous
          )
        ) {
          continue;
        }

        const distance =
          Math.abs(
            cluster.midpoint -
            previous.midpoint
          );

        if (
          distance <
          bestDistance
        ) {

          bestDistance =
            distance;

          matchIndex =
            i;
        }
      }

      if (
        matchIndex >= 0
      ) {

        const item =
          existing[
            matchIndex
          ];

        used.add(
          matchIndex
        );

        const previousUsd =
          item.currentUsd;

        item.lastSeen =
          timestamp;

        item.currentUsd =
          cluster.usd;

        item.maxUsd =
          Math.max(
            item.maxUsd,
            cluster.usd
          );

        item.observations++;

        item.priceLow =
          cluster.priceLow;

        item.priceHigh =
          cluster.priceHigh;

        item.midpoint =
          cluster.midpoint;

        item.levels =
          cluster.levels;

        item.strongBuckets =
          cluster.strongBuckets;

        item.strongestPrice =
          cluster.strongestPrice;

        item.strongestBucketUsd =
          cluster.strongestBucketUsd;

        item.durationMs =
          timestamp -
          item.firstSeen;

        item.previousUsd =
          previousUsd;

        item.changeUsd =
          cluster.usd -
          previousUsd;

        item.status =
          persistenceState(
            item,
            cluster.usd
          );

        /*
           After 60 seconds it is
           officially persistent.
        */

        if (
          item.durationMs >=
          SETTINGS.persistenceConfirmMs
        ) {

          if (
            item.status ===
            'NEW'
          ) {

            item.status =
              'HOLDING';
          }
        }

      } else {

        const item = {

          id:
            `${side}-${Math.round(cluster.midpoint)}-${timestamp}`,

          side:
            bookSide,

          priceLow:
            cluster.priceLow,

          priceHigh:
            cluster.priceHigh,

          midpoint:
            cluster.midpoint,

          strongestPrice:
            cluster.strongestPrice,

          strongestBucketUsd:
            cluster.strongestBucketUsd,

          strongBuckets:
            cluster.strongBuckets,

          levels:
            cluster.levels,

          firstSeen:
            timestamp,

          lastSeen:
            timestamp,

          durationMs:
            0,

          currentUsd:
            cluster.usd,

          maxUsd:
            cluster.usd,

          previousUsd:
            0,

          changeUsd:
            0,

          observations:
            1,

          status:
            'NEW',

          removed:
            false
        };

        existing.push(
          item
        );
      }
    }

    /*
       Detect disappeared clusters.
    */

    for (
      let i =
        existing.length - 1;
      i >= 0;
      i--
    ) {

      const item =
        existing[i];

      if (
        item.removed
      ) {
        continue;
      }

      const age =
        timestamp -
        item.lastSeen;

      if (
        age >
        SETTINGS.persistencePollMs * 1.5
      ) {

        item.removed =
          true;

        item.status =
          'REMOVED';

        item.currentUsd =
          0;

        /*
           Actual lifetime:
           first observation -> last
           observation.
        */

        item.durationMs =
          Math.max(
            0,
            item.lastSeen -
            item.firstSeen
          );

        /*
           Do not promote one-snapshot
           noise into a REMOVED event.
        */

        if (
          item.observations >=
            SETTINGS.minimumRemovedObservations ||
          item.durationMs >=
            SETTINGS.minimumRemovedLifetimeMs
        ) {

          correlateLiquidityEvent(
            item,
            'REMOVED'
          );
        }
      }

      /*
         Remove old history.
      */

      if (
        timestamp -
          item.lastSeen >
        SETTINGS.persistenceMaxAgeMs
      ) {

        existing.splice(
          i,
          1
        );
      }
    }

    persistence[side] =
      existing;
  }

  /*
     Generate persistent events.
  */

  for (
    const side
    of ['bids', 'asks']
  ) {

    for (
      const item
      of persistence[side]
    ) {

      if (
        item.removed
      ) {
        continue;
      }

      if (
        item.durationMs <
        SETTINGS.persistenceConfirmMs
      ) {
        continue;
      }

      if (
        item.status ===
        'HOLDING'
      ) {

        correlateLiquidityEvent(
          item,
          'PERSISTENT'
        );

      } else if (
        item.status ===
        'STRENGTHENING'
      ) {

        correlateLiquidityEvent(
          item,
          'STRENGTHENING'
        );

      } else if (
        item.status ===
        'WEAKENING'
      ) {

        correlateLiquidityEvent(
          item,
          'WEAKENING'
        );
      }
    }
  }
}

/* ============================================================
   SWEEP EXISTS
============================================================ */

function sweepExists(
  side,
  level,
  candleTime
) {

  return state.sweeps.some(
    s =>
      s.side === side &&
      s.level ===
        round(
          level.price,
          2
        ) &&
      s.candleTime ===
        candleTime
  );
}

/* ============================================================
   TRUE SWEEP DETECTION
============================================================ */

function detectCandleSweeps(
  timeframe,
  candle
) {

  if (
    !candle ||
    !Number.isFinite(
      candle.high
    ) ||
    !Number.isFinite(
      candle.low
    ) ||
    !Number.isFinite(
      candle.close
    )
  ) {

    return;
  }

  /*
     IMPORTANT:

     Use the ACTIVE/STABLE structure
     BEFORE rebuilding it.

     This prevents a rebuild from
     deleting the very level needed
     to identify the sweep.
  */

  const levels =
    structure.levels
      .filter(
        isMajorLevel
      );

  for (
    const level
    of levels
  ) {

    const penetration =
      level.price *
      SETTINGS.sweepMinPenetrationPct;

    /*
       BSL sweep:
       trade above entire zone,
       then close below zone.
    */

    if (
      level.side === 'BSL'
    ) {

      const tradedAbove =
        candle.high >=
        level.priceHigh +
        penetration;

      const reclaimed =
        candle.close <
        level.priceHigh;

      if (
        tradedAbove &&
        reclaimed &&
        !sweepExists(
          'BSL',
          level,
          candle.openTime
        )
      ) {

        recordSweep(
          'BSL',
          level,
          candle.high,
          timeframe,
          candle.openTime,
          true
        );
      }
    }

    /*
       SSL sweep:
       trade below entire zone,
       then close above zone.
    */

    if (
      level.side === 'SSL'
    ) {

      const tradedBelow =
        candle.low <=
        level.priceLow -
        penetration;

      const reclaimed =
        candle.close >
        level.priceLow;

      if (
        tradedBelow &&
        reclaimed &&
        !sweepExists(
          'SSL',
          level,
          candle.openTime
        )
      ) {

        recordSweep(
          'SSL',
          level,
          candle.low,
          timeframe,
          candle.openTime,
          true
        );
      }
    }
  }
}

/* ============================================================
   RECORD SWEEP
============================================================ */

function recordSweep(
  side,
  level,
  sweepPrice,
  timeframe,
  candleTime,
  reclaimed
) {

  const event = {

    side,

    level:
      round(
        level.price,
        2
      ),

    zoneLow:
      round(
        level.priceLow,
        2
      ),

    zoneHigh:
      round(
        level.priceHigh,
        2
      ),

    sweepPrice:
      round(
        sweepPrice,
        2
      ),

    timeframe,

    candleTime,

    status:
      'SWEPT',

    reclaimed:
      !!reclaimed,

    detectedAt:
      now(),

    timeframes:
      level.timeframes
  };

  state.sweeps.unshift(
    event
  );

  state.sweeps =
    state.sweeps
      .filter(
        s =>
          now() -
            s.detectedAt <=
          SETTINGS.sweepLookbackMs
      )
      .slice(
        0,
        50
      );

  state.liquidityEvents.unshift({

    key:
      `SWEEP|${side}|${level.price}|${candleTime}`,

    type:
      'SWEEP',

    time:
      now(),

    side,

    structuralLevel:
      round(
        level.price,
        2
      ),

    zoneLow:
      round(
        level.priceLow,
        2
      ),

    zoneHigh:
      round(
        level.priceHigh,
        2
      ),

    sweepPrice:
      round(
        sweepPrice,
        2
      ),

    timeframe,

    candleTime,

    message:
      `${side} SWEPT / RECLAIMED`
  });

  state.liquidityEvents =
    state.liquidityEvents.slice(
      0,
      SETTINGS.eventMax
    );
}

/* ============================================================
   PUBLIC STRUCTURAL LEVELS
============================================================ */

function structural(side) {

  applyResting();

  return structure.levels

    .filter(
      level =>
        level.side === side &&
        isMajorLevel(level)
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

function nearStructural(side) {

  return structure.levels

    .filter(
      level =>
        level.side === side &&
        isNearLevel(level)
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
   PUBLIC LEVEL
============================================================ */

function publicLevel(level) {

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
      level.distancePct,

    timeframeWeight:
      level.timeframeWeight
  };
}

/* ============================================================
   PUBLIC PERSISTENCE
============================================================ */

function publicPersistence(
  side
) {

  return persistence[side]

    .filter(
      item =>
        item.currentUsd >=
          SETTINGS.orderMinUsd ||
        item.removed
    )

    .sort(
      (a, b) =>
        b.currentUsd -
        a.currentUsd
    )

    .slice(
      0,
      20
    )

    .map(
      item => {

        const structuralLevel =
          structuralNearBookCluster(
            item
          );

        return {

          side:
            item.side,

          priceLow:
            round(
              item.priceLow,
              2
            ),

          priceHigh:
            round(
              item.priceHigh,
              2
            ),

          midpoint:
            round(
              item.midpoint,
              2
            ),

          strongestPrice:
            round(
              item.strongestPrice,
              2
            ),

          strongestBucketUsd:
            round(
              item.strongestBucketUsd || 0,
              2
            ),

          usd:
            round(
              item.currentUsd,
              2
            ),

          maxUsd:
            round(
              item.maxUsd,
              2
            ),

          previousUsd:
            round(
              item.previousUsd || 0,
              2
            ),

          changeUsd:
            round(
              item.changeUsd || 0,
              2
            ),

          levels:
            item.levels,

          strongBuckets:
            item.strongBuckets || 0,

          firstSeen:
            safeDate(
              item.firstSeen
            ),

          lastSeen:
            safeDate(
              item.lastSeen
            ),

          durationMs:
            item.durationMs,

          durationSec:
            round(
              item.durationMs /
                1000,
              0
            ),

          observations:
            item.observations,

          status:
            item.status,

          removed:
            item.removed,

          persistent:
            item.durationMs >=
            SETTINGS.persistenceConfirmMs,

          structuralConfluence:
            !!structuralLevel,

          structuralLevel:
            structuralLevel
              ? round(
                  structuralLevel.price,
                  2
                )
              : null,

          structuralZoneLow:
            structuralLevel
              ? round(
                  structuralLevel.priceLow,
                  2
                )
              : null,

          structuralZoneHigh:
            structuralLevel
              ? round(
                  structuralLevel.priceHigh,
                  2
                )
              : null,

          structuralTimeframes:
            structuralLevel
              ? structuralLevel.timeframes
              : []
        };
      }
    );
}

/* ============================================================
   INTELLIGENCE
============================================================ */

function intelligence() {

  applyResting();

  if (!state.currentPrice) {

    return {

      state:
        'WAITING FOR LIVE PRICE'
    };
  }

  const bsl =
    structural('BSL')
      .filter(
        level =>
          level.price >
          state.currentPrice
      );

  const ssl =
    structural('SSL')
      .filter(
        level =>
          level.price <
          state.currentPrice
      );

  const nearBsl =
    nearStructural('BSL')
      .filter(
        level =>
          level.price >
          state.currentPrice
      );

  const nearSsl =
    nearStructural('SSL')
      .filter(
        level =>
          level.price <
          state.currentPrice
      );

  let status =
    'NO MAJOR STRUCTURAL LIQUIDITY';

  if (
    bsl.length &&
    ssl.length
  ) {

    status =
      'BETWEEN MAJOR LIQUIDITY';

  } else if (
    bsl.length
  ) {

    status =
      'MAJOR UPSIDE LIQUIDITY ONLY';

  } else if (
    ssl.length
  ) {

    status =
      'MAJOR DOWNSIDE LIQUIDITY ONLY';
  }

  return {

    state:
      status,

    structureStatus: {

      activeLevels:
        structure.levels.length,

      activeMajorBSL:
        bsl.length,

      activeMajorSSL:
        ssl.length,

      lastRebuildAt:
        safeDate(
          structure.lastRebuildAt
        ),

      lastGoodAt:
        safeDate(
          structure.lastGoodAt
        ),

      lastRebuildAccepted:
        structure.lastRebuildAccepted,

      lastRebuildReason:
        structure.lastRebuildReason,

      rebuildCount:
        structure.rebuildCount
    },

    nearestBSL:
      bsl[0]
        ? publicLevel(bsl[0])
        : null,

    nearestSSL:
      ssl[0]
        ? publicLevel(ssl[0])
        : null,

    nextBSL:
      bsl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    nextSSL:
      ssl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    nearBSL:
      nearBsl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    nearSSL:
      nearSsl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    orderBook: {

      bids:
        bookClusters('bid')
          .slice(0, 10)
          .map(
            cluster => ({

              side:
                cluster.side,

              priceLow:
                round(
                  cluster.priceLow,
                  2
                ),

              priceHigh:
                round(
                  cluster.priceHigh,
                  2
                ),

              midpoint:
                round(
                  cluster.midpoint,
                  2
                ),

              usd:
                round(
                  cluster.usd,
                  2
                ),

              levels:
                cluster.levels,

              strongBuckets:
                cluster.strongBuckets,

              strongestPrice:
                round(
                  cluster.strongestPrice,
                  2
                ),

              strongestBucketUsd:
                round(
                  cluster.strongestBucketUsd,
                  2
                )
            })
          ),

      asks:
        bookClusters('ask')
          .slice(0, 10)
          .map(
            cluster => ({

              side:
                cluster.side,

              priceLow:
                round(
                  cluster.priceLow,
                  2
                ),

              priceHigh:
                round(
                  cluster.priceHigh,
                  2
                ),

              midpoint:
                round(
                  cluster.midpoint,
                  2
                ),

              usd:
                round(
                  cluster.usd,
                  2
                ),

              levels:
                cluster.levels,

              strongBuckets:
                cluster.strongBuckets,

              strongestPrice:
                round(
                  cluster.strongestPrice,
                  2
                ),

              strongestBucketUsd:
                round(
                  cluster.strongestBucketUsd,
                  2
                )
            })
          )
    },

    persistentLiquidity: {

      bids:
        publicPersistence(
          'bids'
        ),

      asks:
        publicPersistence(
          'asks'
        )
    },

    liquidityEvents:
      state.liquidityEvents
        .slice(0, 20),

    recentSweeps:
      state.sweeps
        .slice(0, 20)
  };
}

/* ============================================================
   PUBLIC STRUCTURE
============================================================ */

function publicStructure() {

  applyResting();

  const bsl =
    structural('BSL');

  const ssl =
    structural('SSL');

  return {

    buySideLiquidity:
      bsl.map(
        publicLevel
      ),

    sellSideLiquidity:
      ssl.map(
        publicLevel
      ),

    majorResistance:
      bsl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    majorSupport:
      ssl
        .slice(0, 5)
        .map(
          publicLevel
        ),

    nearestBSL:
      bsl[0]
        ? publicLevel(
            bsl[0]
          )
        : null,

    nearestSSL:
      ssl[0]
        ? publicLevel(
            ssl[0]
          )
        : null,

    nearBSL:
      nearStructural('BSL')
        .map(
          publicLevel
        ),

    nearSSL:
      nearStructural('SSL')
        .map(
          publicLevel
        ),

    status: {

      activeLevels:
        structure.levels.length,

      lastGoodAt:
        safeDate(
          structure.lastGoodAt
        ),

      lastRebuildAt:
        safeDate(
          structure.lastRebuildAt
        ),

      lastRebuildAccepted:
        structure.lastRebuildAccepted,

      lastRebuildReason:
        structure.lastRebuildReason,

      rebuildCount:
        structure.rebuildCount
    }
  };
}

/* ============================================================
   PUBLIC STATS
============================================================ */

function publicStats() {

  return {

    candles:
      state.candleStats,

    structure: {

      swingHighs:
        structure.swingHighs.length,

      swingLows:
        structure.swingLows.length,

      levels:
        structure.levels.length,

      cachedLevels:
        structure.lastGoodLevels.length,

      majorBSL:
        structural('BSL').length,

      majorSSL:
        structural('SSL').length
    },

    orderBook: {

      bids:
        book.bids.size,

      asks:
        book.asks.size,

      persistentBids:
        persistence.bids.length,

      persistentAsks:
        persistence.asks.length
    }
  };
}

/* ============================================================
   SNAPSHOT APPLY
============================================================ */

function snapshotBookApply(
  payload
) {

  book.bids.clear();
  book.asks.clear();

  for (
    const [
      priceString,
      qtyString
    ]
    of payload.bids || []
  ) {

    const price =
      Number(
        priceString
      );

    const qty =
      Number(
        qtyString
      );

    if (
      qty > 0
    ) {

      book.bids.set(
        price,
        qty
      );
    }
  }

  for (
    const [
      priceString,
      qtyString
    ]
    of payload.asks || []
  ) {

    const price =
      Number(
        priceString
      );

    const qty =
      Number(
        qtyString
      );

    if (
      qty > 0
    ) {

      book.asks.set(
        price,
        qty
      );
    }
  }

  state.lastUpdateId =
    Number(
      payload.lastUpdateId || 0
    );

  state.bookInitialized =
    true;

  state.gapDetected =
    false;

  state.gapRecovery =
    false;

  state.snapshot.pending =
    false;

  state.snapshot.lastSnapshotId =
    state.lastUpdateId;

  updatePriceFromBook();
}

/* ============================================================
   DEPTH EVENT
============================================================ */

function applyDepthEvent(e) {

  const U =
    Number(e.U);

  const u =
    Number(e.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  /*
     Buffer until snapshot exists.
  */

  if (
    !state.bookInitialized
  ) {

    state.pendingEvents.push(
      e
    );

    if (
      state.pendingEvents.length >
      20000
    ) {

      state.pendingEvents.shift();
    }

    return;
  }

  /*
     Old event.
  */

  if (
    u <=
    state.lastUpdateId
  ) {

    return;
  }

  /*
     Sequence gap.
  */

  if (
    U >
    state.lastUpdateId + 1
  ) {

    state.gapDetected =
      true;

    state.gapRecovery =
      true;

    state.gapCount++;

    state.lastGapExpected =
      state.lastUpdateId + 1;

    state.lastGapReceived =
      U;

    state.pendingEvents.push(
      e
    );

    requestSnapshot(
      'sequence_gap'
    );

    return;
  }

  /*
     Bids.
  */

  for (
    const [
      priceString,
      qtyString
    ]
    of e.b || []
  ) {

    const price =
      Number(
        priceString
      );

    const qty =
      Number(
        qtyString
      );

    if (
      qty === 0
    ) {

      book.bids.delete(
        price
      );

    } else {

      book.bids.set(
        price,
        qty
      );
    }
  }

  /*
     Asks.
  */

  for (
    const [
      priceString,
      qtyString
    ]
    of e.a || []
  ) {

    const price =
      Number(
        priceString
      );

    const qty =
      Number(
        qtyString
      );

    if (
      qty === 0
    ) {

      book.asks.delete(
        price
      );

    } else {

      book.asks.set(
        price,
        qty
      );
    }
  }

  state.lastUpdateId =
    u;

  updatePriceFromBook();
}

/* ============================================================
   SNAPSHOT BRIDGE
============================================================ */

function bridgePendingEvents(
  snapshotId
) {

  state.snapshot.bridgeAttempts++;

  const pending =
    state.pendingEvents
      .sort(
        (a, b) =>
          Number(a.u) -
          Number(b.u)
      );

  const index =
    pending.findIndex(
      event =>
        Number(event.U) <=
          snapshotId + 1 &&
        Number(event.u) >=
          snapshotId + 1
    );

  if (
    index < 0
  ) {

    state.snapshot.pending =
      false;

    state.gapRecovery =
      true;

    return false;
  }

  state.snapshot.bridgeFound++;

  state.lastUpdateId =
    snapshotId;

  for (
    let i = index;
    i < pending.length;
    i++
  ) {

    const event =
      pending[i];

    if (
      Number(event.u) <=
      state.lastUpdateId
    ) {
      continue;
    }

    if (
      Number(event.U) >
      state.lastUpdateId + 1
    ) {

      state.gapDetected =
        true;

      state.gapRecovery =
        true;

      state.lastGapExpected =
        state.lastUpdateId + 1;

      state.lastGapReceived =
        Number(event.U);

      return false;
    }

    for (
      const [
        priceString,
        qtyString
      ]
      of event.b || []
    ) {

      const price =
        Number(
          priceString
        );

      const qty =
        Number(
          qtyString
        );

      if (
        qty === 0
      ) {

        book.bids.delete(
          price
        );

      } else {

        book.bids.set(
          price,
          qty
        );
      }
    }

    for (
      const [
        priceString,
        qtyString
      ]
      of event.a || []
    ) {

      const price =
        Number(
          priceString
        );

      const qty =
        Number(
          qtyString
        );

      if (
        qty === 0
      ) {

        book.asks.delete(
          price
        );

      } else {

        book.asks.set(
          price,
          qty
        );
      }
    }

    state.lastUpdateId =
      Number(
        event.u
      );
  }

  state.pendingEvents = [];

  state.bookInitialized =
    true;

  state.gapRecovery =
    false;

  state.gapDetected =
    false;

  updatePriceFromBook();

  return true;
}

/* ============================================================
   SNAPSHOT REQUEST
============================================================ */

function requestSnapshot(
  reason = 'startup'
) {

  if (
    state.snapshot.pending
  ) {
    return;
  }

  if (
    now() -
      state.snapshot.lastRequestAt <
    5000
  ) {
    return;
  }

  state.snapshot.pending =
    true;

  state.snapshot.lastRequestAt =
    now();

  state.snapshot.snapshotRequests++;

  state.snapshot.lastError =
    null;

  const ws =
    new WebSocket(
      SNAPSHOT_WS
    );

  const timer =
    setTimeout(
      () => {

        try {
          ws.close();
        } catch {}

      },
      8000
    );

  ws.on(
    'open',
    () => {

      state.snapshotConnected =
        true;

      const id =
        `depth-${Date.now()}-${Math.random()
          .toString(36)
          .slice(2, 8)}`;

      ws.send(
        JSON.stringify({

          id,

          method:
            'depth',

          params: {

            symbol:
              SYMBOL,

            limit:
              1000
          }
        })
      );
    }
  );

  ws.on(
    'message',
    data => {

      try {

        const msg =
          JSON.parse(
            data.toString()
          );

        if (
          msg.status ===
          429
        ) {

          state.snapshot.snapshot429s++;
        }

        if (
          msg.status &&
          msg.status >= 400
        ) {

          throw new Error(
            `Snapshot HTTP ${msg.status}`
          );
        }

        if (
          !msg.result ||
          !msg.result.lastUpdateId
        ) {

          return;
        }

        clearTimeout(
          timer
        );

        snapshotBookApply(
          msg.result
        );

        const ok =
          bridgePendingEvents(
            Number(
              msg.result.lastUpdateId
            )
          );

        if (!ok) {

          state.snapshot.lastError =
            `Unable to bridge snapshot (${reason})`;
        }

        try {
          ws.close();
        } catch {}

      } catch (err) {

        state.snapshot.lastError =
          err.message;

        state.snapshot.pending =
          false;
      }
    }
  );

  ws.on(
    'error',
    err => {

      clearTimeout(
        timer
      );

      state.snapshot.lastError =
        err.message;

      state.snapshot.pending =
        false;
    }
  );

  ws.on(
    'close',
    () => {

      state.snapshotConnected =
        false;

      clearTimeout(
        timer
      );

      if (
        state.snapshot.pending &&
        now() -
          state.snapshot.lastRequestAt >
          7000
      ) {

        state.snapshot.pending =
          false;
      }
    }
  );
}

/* ============================================================
   DEPTH WEBSOCKET
============================================================ */

function connectDepth() {

  const ws =
    new WebSocket(
      DEPTH_WS
    );

  ws.on(
    'open',
    () => {

      state.depthConnected =
        true;

      state.lastDepthMessage =
        now();

      console.log(
        'Depth websocket connected'
      );

      requestSnapshot(
        'startup'
      );
    }
  );

  ws.on(
    'message',
    data => {

      state.lastDepthMessage =
        now();

      try {

        applyDepthEvent(
          JSON.parse(
            data.toString()
          )
        );

      } catch (err) {

        console.error(
          'Depth parse:',
          err.message
        );
      }
    }
  );

  ws.on(
    'close',
    () => {

      state.depthConnected =
        false;

      console.log(
        'Depth websocket closed; reconnecting'
      );

      setTimeout(
        connectDepth,
        3000
      );
    }
  );

  ws.on(
    'error',
    err => {

      console.error(
        'Depth websocket:',
        err.message
      );
    }
  );
}

/* ============================================================
   CANDLE UPDATE
============================================================ */

function upsertCandle(
  tf,
  candle,
  closed
) {

  const arr =
    candles[tf];

  if (
    !arr.length ||
    arr.at(-1).openTime !==
      candle.openTime
  ) {

    arr.push(
      candle
    );

  } else {

    arr[arr.length - 1] =
      candle;
  }

  const max =
    STRUCTURE[tf].candles;

  if (
    arr.length > max
  ) {

    arr.splice(
      0,
      arr.length - max
    );
  }

  state.candleStats[tf].loaded =
    arr.length;

  state.candleStats[tf].lastOpenTime =
    candle.openTime;

  /*
     IMPORTANT:

     Sweep detection happens BEFORE
     structure rebuild.
  */

  if (closed) {

    detectCandleSweeps(
      tf,
      candle
    );

    rebuildStructure(
      `closed_${tf}`
    );
  }
}

/* ============================================================
   KLINE WEBSOCKET
============================================================ */

function connectKlines() {

  const ws =
    new WebSocket(
      KLINE_WS
    );

  ws.on(
    'open',
    () => {

      state.klineConnected =
        true;

      console.log(
        'Kline websocket connected'
      );
    }
  );

  ws.on(
    'message',
    data => {

      state.lastKlineMessage =
        now();

      try {

        const msg =
          JSON.parse(
            data.toString()
          );

        const k =
          msg.data?.k;

        if (!k) {
          return;
        }

        const tf =
          String(k.i);

        if (!candles[tf]) {
          return;
        }

        const candle =
          candleFromKline(
            k
          );

        upsertCandle(
          tf,
          candle,
          !!k.x
        );

        if (
          !state.bookInitialized
        ) {

          updatePrice(
            candle.close,
            'futures_kline'
          );
        }

      } catch (err) {

        console.error(
          'Kline parse:',
          err.message
        );
      }
    }
  );

  ws.on(
    'close',
    () => {

      state.klineConnected =
        false;

      console.log(
        'Kline websocket closed; reconnecting'
      );

      setTimeout(
        connectKlines,
        3000
      );
    }
  );

  ws.on(
    'error',
    err => {

      console.error(
        'Kline websocket:',
        err.message
      );
    }
  );
}

/* ============================================================
   ROOT
============================================================ */

app.get(
  '/',
  (req, res) => {

    res.json({

      ok: true,

      service:
        'Binance Futures Major Liquidity Relay',

      symbol:
        SYMBOL,

      endpoints: [

        '/health',

        '/intelligence',

        '/structure',

        '/liquidity',

        '/book',

        '/events'
      ]
    });
  }
);

/* ============================================================
   HEALTH
============================================================ */

app.get(
  '/health',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      status:
        state.bookInitialized
          ? 'live'
          : (
              state.snapshot.pending
                ? 'syncing'
                : 'waiting'
            ),

      initialized:
        state.bookInitialized,

      websocketConnected:
        state.depthConnected,

      depthConnected:
        state.depthConnected,

      snapshotConnected:
        state.snapshotConnected,

      klineConnected:
        state.klineConnected,

      snapshotPending:
        state.snapshot.pending,

      waitingForBridge:
        !state.bookInitialized &&
        state.pendingEvents.length > 0,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      bidLevels:
        book.bids.size,

      askLevels:
        book.asks.size,

      pendingEvents:
        state.pendingEvents.length,

      lastUpdateId:
        state.lastUpdateId,

      gapDetected:
        state.gapDetected,

      gapRecovery:
        state.gapRecovery,

      gapCount:
        state.gapCount,

      lastGapExpected:
        state.lastGapExpected,

      lastGapReceived:
        state.lastGapReceived,

      snapshot:
        state.snapshot,

      structure: {

        activeLevels:
          structure.levels.length,

        cachedLevels:
          structure.lastGoodLevels.length,

        majorBSL:
          structural('BSL').length,

        majorSSL:
          structural('SSL').length,

        lastRebuildAt:
          safeDate(
            structure.lastRebuildAt
          ),

        lastGoodAt:
          safeDate(
            structure.lastGoodAt
          ),

        lastRebuildAccepted:
          structure.lastRebuildAccepted,

        lastRebuildReason:
          structure.lastRebuildReason,

        rebuildCount:
          structure.rebuildCount
      },

      persistence: {

        bids:
          persistence.bids.length,

        asks:
          persistence.asks.length,

        confirmMs:
          SETTINGS.persistenceConfirmMs,

        confirmSec:
          SETTINGS.persistenceConfirmMs /
          1000
      },

      sweeps:
        state.sweeps.length,

      liquidityEvents:
        state.liquidityEvents.length,

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   INTELLIGENCE
============================================================ */

app.get(
  '/intelligence',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      intelligence:
        intelligence(),

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   STRUCTURE
============================================================ */

app.get(
  '/structure',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      marketStructure:
        publicStructure(),

      intelligence:
        intelligence(),

      stats:
        publicStats(),

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   LIQUIDITY
============================================================ */

app.get(
  '/liquidity',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      marketStructure:
        publicStructure(),

      intelligence:
        intelligence(),

      rawRestingLiquidity: {

        largeBids:
          bookRows('bid')
            .slice(0, 50)
            .map(
              x => ({

                price:
                  round(
                    x.price,
                    2
                  ),

                quantity:
                  round(
                    x.quantity,
                    6
                  ),

                usd:
                  round(
                    x.usd,
                    2
                  )
              })
            ),

        largeAsks:
          bookRows('ask')
            .slice(0, 50)
            .map(
              x => ({

                price:
                  round(
                    x.price,
                    2
                  ),

                quantity:
                  round(
                    x.quantity,
                    6
                  ),

                usd:
                  round(
                    x.usd,
                    2
                  )
              })
            )
      },

      stats:
        publicStats(),

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   RAW BOOK
============================================================ */

app.get(
  '/book',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      bids:
        bookRows('bid')
          .slice(0, 1000),

      asks:
        bookRows('ask')
          .slice(0, 1000),

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   EVENTS
============================================================ */

app.get(
  '/events',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      recentSweeps:
        state.sweeps
          .slice(0, 50),

      liquidityEvents:
        state.liquidityEvents
          .slice(0, 50),

      persistentLiquidity: {

        bids:
          publicPersistence(
            'bids'
          ),

        asks:
          publicPersistence(
            'asks'
          )
      },

      updatedAt:
        state.updatedAt
    });
  }
);

/* ============================================================
   PERSISTENCE / MAINTENANCE
============================================================ */

setInterval(
  () => {

    /*
       Recover order book if necessary.
    */

    if (
      state.depthConnected &&
      (
        !state.bookInitialized ||
        state.gapRecovery
      )
    ) {

      requestSnapshot(
        state.gapRecovery
          ? 'recovery'
          : 'maintenance'
      );
    }

    if (
      state.bookInitialized
    ) {

      updatePriceFromBook();

      applyResting();

      updatePersistence();
    }

  },
  SETTINGS.persistencePollMs
);

/* ============================================================
   STRUCTURE MAINTENANCE
============================================================ */

setInterval(
  () => {

    if (
      Object.values(candles)
        .every(
          arr =>
            arr.length
        )
    ) {

      rebuildStructure(
        'maintenance'
      );
    }

  },
  60000
);

/* ============================================================
   START
============================================================ */

(async () => {

  await loadHistory();

  connectDepth();

  connectKlines();

  app.listen(
    PORT,
    () => {

      console.log(
        `Binance Futures Major Liquidity Relay listening on ${PORT}`
      );
    }
  );

})().catch(
  err => {

    console.error(
      'Startup failed:',
      err
    );

    process.exit(1);
  }
);
