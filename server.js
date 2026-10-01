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
   GENERAL SETTINGS
============================================================ */

const SETTINGS = {

  /* Raw order book */
  rawMinUsd: 50000,

  /* Structural level resting liquidity */
  restingRadiusUsd: 35,
  restingConfirmUsd: 250000,

  /* Structure merging */
  mergeUsd: 35,
  zonePct: 0.0015,
  reactionPct: 0.003,

  /* Structure */
  minScore: 40,
  maxLevels: 8,

  /* Order-book clustering */
  orderGapUsd: 35,
  orderMinUsd: 500000,
  minClusterLevels: 3,

  /* Persistence */
  persistencePollMs: 5000,
  persistenceConfirmMs: 60000,
  persistenceMaxAgeMs: 10 * 60 * 1000,
  persistenceTolerancePct: 0.0005,

  /* Events */
  eventMax: 100,

  /* Sweeps */
  sweepLookbackMs: 24 * 60 * 60 * 1000,

  /*
     Minimum penetration beyond the structural zone.
     0.00015 = 0.015%
  */
  sweepMinPenetrationPct: 0.00015,

  /* Major liquidity filtering */
  majorMinDistancePct: 0.50,
  majorMinimumTimeframeWeight: 2,
  major1hMinScore: 90,
  majorHigherTfMinScore: 40
};

/* ============================================================
   DATA
============================================================ */

const candles = {
  '15m': [],
  '1h': [],
  '4h': [],
  '1d': []
};

const book = {
  bids: new Map(),
  asks: new Map()
};

const structure = {
  swingHighs: [],
  swingLows: [],
  equalHighs: [],
  equalLows: [],
  levels: []
};

/*
   Persistence objects are retained across snapshots.

   bids = resting bid clusters
   asks = resting ask clusters
*/
const persistence = {
  bids: [],
  asks: []
};

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

  /*
     True structural sweep events
  */
  sweeps: [],

  /*
     Liquidity events:
     - CONFLUENCE
     - PERSISTENT LIQUIDITY
     - LIQUIDITY REMOVED
     - SWEEP
  */
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

/* ============================================================
   PRICE
============================================================ */

function updatePrice(price, source) {

  const p = Number(price);

  if (!Number.isFinite(p) || p <= 0) {
    return;
  }

  state.currentPrice = p;
  state.priceSource = source;
  state.updatedAt = new Date().toISOString();
}

function updatePriceFromBook() {

  if (!book.bids.size || !book.asks.size) {
    return;
  }

  const bids = [...book.bids.keys()]
    .sort((a, b) => b - a);

  const asks = [...book.asks.keys()]
    .sort((a, b) => a - b);

  if (!bids.length || !asks.length) {
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
   HISTORICAL SPOT DATA
============================================================ */

async function fetchSpotKlines(interval, limit) {

  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${limit}`;

  const res = await fetch(url);

  if (!res.ok) {
    throw new Error(
      `Spot klines ${interval}: HTTP ${res.status}`
    );
  }

  const rows = await res.json();

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

  for (const [tf, cfg] of Object.entries(STRUCTURE)) {

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

  rebuildStructure();
}

/* ============================================================
   PIVOTS
============================================================ */

function isPivotHigh(arr, i, left, right) {

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

function isPivotLow(arr, i, left, right) {

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
    Math.max(0, index - 8);

  let lastReaction = -Infinity;

  for (
    let i = start;
    i < Math.min(arr.length, index + 9);
    i++
  ) {

    if (i === index) {
      continue;
    }

    const c = arr[i];

    const tolerance =
      price * SETTINGS.reactionPct;

    const touched =
      side === 'BSL'
        ? c.high >= price - tolerance
        : c.low <= price + tolerance;

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
        price: arr[i].high,
        time: arr[i].openTime,
        index: i,

        timeframe: tf,
        weight: cfg.weight,

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
        price: arr[i].low,
        time: arr[i].openTime,
        index: i,

        timeframe: tf,
        weight: cfg.weight,

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
   STRUCTURE CLUSTERING
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

  let score =
    weights * 10 +
    Math.min(equalCount, 30) * 5 +
    Math.min(reactions, 40) * 2;

  const level = {

    side: group.side,

    price:
      group.points.reduce(
        (s, x) => s + x.price,
        0
      ) /
      group.points.length,

    priceLow: zoneLow,
    priceHigh: zoneHigh,

    score,

    scoreBase: score,

    strength: 'LOW',

    timeframes: tfs,

    equalCount,

    reactions,

    restingUsd: 0,

    restingConfirmed: false,

    restingLevels: [],

    distancePct: null,

    timeframeWeight: weights
  };

  if (
    score >= 125 ||
    (
      tfs.length >= 3 &&
      equalCount >= 4
    )
  ) {

    level.strength = 'HIGH';

  } else if (
    score >= 90 ||
    tfs.length >= 2 ||
    equalCount >= 3
  ) {

    level.strength = 'MEDIUM';
  }

  return level;
}

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

  for (const p of sorted) {

    let best = null;

    for (const g of groups) {

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

      if (
        Math.abs(
          p.price - avg
        ) <= tolerance
      ) {

        best = g;
        break;
      }
    }

    if (!best) {

      groups.push({
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
   MAJOR LEVEL FILTER
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
      (level.price -
        state.currentPrice) /
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
    level.timeframes.includes('1h');

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
      (level.price -
        state.currentPrice) /
        state.currentPrice
    ) * 100
    <
    SETTINGS.majorMinDistancePct
  );
}

/* ============================================================
   STRUCTURE REBUILD
============================================================ */

function rebuildStructure() {

  const allHighs = [];
  const allLows = [];

  structure.swingHighs = [];
  structure.swingLows = [];

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

    structure.swingHighs.push(
      ...s.highs
    );

    structure.swingLows.push(
      ...s.lows
    );

    allHighs.push(
      ...s.highs
    );

    allLows.push(
      ...s.lows
    );
  }

  structure.equalHighs =
    allHighs.filter(
      p => p.reactions >= 1
    );

  structure.equalLows =
    allLows.filter(
      p => p.reactions >= 1
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

  structure.levels = [
    ...highs,
    ...lows
  ];

  applyResting();
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
        quantity: qty,
        usd: price * qty
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
   STRUCTURAL RESTING LIQUIDITY
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
    const level of structure.levels
  ) {

    const r =
      restingNear(
        level.price,
        level.side
      );

    level.restingUsd =
      round(r.usd, 2);

    level.restingLevels =
      r.levels
        .slice(0, 20)
        .map(
          x => ({
            price:
              round(x.price, 2),

            quantity:
              round(x.quantity, 6),

            usd:
              round(x.usd, 2)
          })
        );

    level.restingConfirmed =
      r.usd >=
      SETTINGS.restingConfirmUsd;

    level.scoreBase =
      level.scoreBase ??
      level.score;

    /*
       Recalculate from structural
       score instead of repeatedly
       stacking bonuses.
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

    if (
      level.score >= 125 ||
      (
        level.timeframes.length >= 3 &&
        level.equalCount >= 4
      )
    ) {

      level.strength = 'HIGH';

    } else if (
      level.score >= 90 ||
      level.timeframes.length >= 2 ||
      level.equalCount >= 3
    ) {

      level.strength = 'MEDIUM';

    } else {

      level.strength = 'LOW';
    }

    level.distancePct =
      pctDistance(
        level.price,
        state.currentPrice
      );
  }
}

/* ============================================================
   ORDER BOOK CLUSTERS
============================================================ */

function bookClusters(side) {

  const rows =
    bookRows(side);

  if (!rows.length) {
    return [];
  }

  const sorted =
    [...rows].sort(
      (a, b) =>
        a.price - b.price
    );

  const clusters = [];

  let current = [];

  for (const row of sorted) {

    if (
      !current.length ||
      row.price -
        current.at(-1).price
        <= SETTINGS.orderGapUsd
    ) {

      current.push(row);

    } else {

      clusters.push(current);
      current = [row];
    }
  }

  if (current.length) {
    clusters.push(current);
  }

  return clusters

    /*
       IMPORTANT:
       A persistence candidate must
       contain at least 3 neighboring
       levels.
    */
    .filter(
      c =>
        c.length >=
        SETTINGS.minClusterLevels
    )

    .map(c => ({

      side,

      priceLow:
        c[0].price,

      priceHigh:
        c.at(-1).price,

      midpoint:
        c.reduce(
          (s, x) =>
            s + x.price,
          0
        ) /
        c.length,

      usd:
        c.reduce(
          (s, x) =>
            s + x.usd,
          0
        ),

      levels:
        c.length,

      strongestPrice:
        c.reduce(
          (a, b) =>
            a.usd >= b.usd
              ? a
              : b
        ).price
    }))

    .filter(
      c =>
        c.usd >=
        SETTINGS.orderMinUsd
    )

    .sort(
      (a, b) =>
        b.usd - a.usd
    );
}

/* ============================================================
   PERSISTENCE MATCHING
============================================================ */

function clusterStillNear(
  current,
  previous
) {

  const tolerance =
    Math.max(
      SETTINGS.orderGapUsd * 2,

      (current.midpoint || 0) *
        SETTINGS.persistenceTolerancePct
    );

  const midpointClose =
    Math.abs(
      current.midpoint -
      previous.midpoint
    ) <= tolerance;

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
   PERSISTENCE STATE
============================================================ */

function persistenceState(
  previous,
  currentUsd
) {

  if (!previous) {
    return 'NEW';
  }

  /*
     Compare current liquidity
     to the previous observation.
  */

  const previousUsd =
    previous.currentUsd || 0;

  if (
    currentUsd >
    previousUsd * 1.15
  ) {

    return 'STRENGTHENING';
  }

  if (
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

function structuralNearBookCluster(p) {

  const structuralSide =
    p.side === 'ask'
      ? 'BSL'
      : 'SSL';

  const candidates =
    structure.levels.filter(
      l =>
        l.side ===
        structuralSide
    );

  let best = null;
  let bestDistance = Infinity;

  for (
    const level of candidates
  ) {

    /*
       Distance from order-book
       cluster to structural zone.
    */

    const distance =
      Math.min(

        Math.abs(
          p.midpoint -
          level.priceLow
        ),

        Math.abs(
          p.midpoint -
          level.priceHigh
        ),

        Math.abs(
          p.midpoint -
          level.price
        )
      );

    const maxDistance =
      Math.max(
        SETTINGS.orderGapUsd * 3,
        level.price * 0.003
      );

    if (
      distance <= maxDistance &&
      distance < bestDistance
    ) {

      best = level;
      bestDistance = distance;
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
     Only persistent/confluence
     events need a structural level.

     Removed liquidity is still
     useful even when it is not
     directly on a major level.
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

  if (type === 'REMOVED') {
    message =
      'LIQUIDITY REMOVED';
  } else if (level) {
    message =
      'CONFLUENCE';
  }

  const event = {

    key,

    type,

    time: now(),

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

    structuralLevel:
      level
        ? round(level.price, 2)
        : null,

    structuralZoneLow:
      level
        ? round(level.priceLow, 2)
        : null,

    structuralZoneHigh:
      level
        ? round(level.priceHigh, 2)
        : null,

    timeframes:
      level
        ? level.timeframes
        : [],

    message
  };

  state.liquidityEvents.unshift(
    event
  );

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

  const timestamp = now();

  for (
    const side of ['bids', 'asks']
  ) {

    const bookSide =
      side === 'bids'
        ? 'bid'
        : 'ask';

    const currentClusters =
      bookClusters(bookSide);

    const existing =
      persistence[side];

    const used =
      new Set();

    /*
       Match current clusters
       against previous clusters.
    */

    for (
      const cluster
      of currentClusters
    ) {

      let matchIndex = -1;
      let bestDistance = Infinity;

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

        if (
          existing[i].removed
        ) {
          continue;
        }

        if (
          !clusterStillNear(
            cluster,
            existing[i]
          )
        ) {
          continue;
        }

        const distance =
          Math.abs(
            cluster.midpoint -
            existing[i].midpoint
          );

        if (
          distance <
          bestDistance
        ) {

          bestDistance =
            distance;

          matchIndex = i;
        }
      }

      /*
         Existing persistent cluster
      */

      if (matchIndex >= 0) {

        const item =
          existing[matchIndex];

        used.add(matchIndex);

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

        item.strongestPrice =
          cluster.strongestPrice;

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
           Once it has survived
           long enough, classify
           as persistent.
        */

        if (
          item.durationMs >=
          SETTINGS.persistenceConfirmMs
        ) {

          if (
            item.status === 'NEW'
          ) {

            item.status =
              'HOLDING';
          }
        }

      } else {

        /*
           New cluster
        */

        const item = {

          id:
            `${side}-${Math.round(cluster.midpoint)}-${timestamp}`,

          side: bookSide,

          priceLow:
            cluster.priceLow,

          priceHigh:
            cluster.priceHigh,

          midpoint:
            cluster.midpoint,

          strongestPrice:
            cluster.strongestPrice,

          levels:
            cluster.levels,

          firstSeen:
            timestamp,

          lastSeen:
            timestamp,

          durationMs: 0,

          currentUsd:
            cluster.usd,

          maxUsd:
            cluster.usd,

          previousUsd: 0,

          changeUsd: 0,

          observations: 1,

          status: 'NEW',

          removed: false
        };

        existing.push(item);
      }
    }

    /*
       Detect clusters that disappeared.
    */

    for (
      let i = existing.length - 1;
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

      /*
         One complete polling cycle
         without seeing the cluster
         = removed.
      */

      if (
        age >
        SETTINGS.persistencePollMs * 1.5
      ) {

        item.removed = true;

        item.status =
          'REMOVED';

        item.currentUsd = 0;

        item.durationMs =
          item.lastSeen -
          item.firstSeen;

        correlateLiquidityEvent(
          item,
          'REMOVED'
        );
      }

      /*
         Keep history temporarily,
         then discard old records.
      */

      if (
        timestamp -
          item.lastSeen >
        SETTINGS.persistenceMaxAgeMs
      ) {

        existing.splice(i, 1);
      }
    }

    persistence[side] =
      existing;
  }

  /*
     Generate persistent/confluence
     events after confirmation.
  */

  for (
    const side
    of ['bids', 'asks']
  ) {

    for (
      const item
      of persistence[side]
    ) {

      if (item.removed) {
        continue;
      }

      if (
        item.durationMs >=
        SETTINGS.persistenceConfirmMs
      ) {

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
}

/* ============================================================
   TRUE SWEEP DETECTION
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
        round(level.price, 2) &&
      s.candleTime ===
        candleTime
  );
}

function detectCandleSweeps(
  timeframe,
  candle
) {

  if (
    !candle ||
    !Number.isFinite(candle.high) ||
    !Number.isFinite(candle.low) ||
    !Number.isFinite(candle.close)
  ) {

    return;
  }

  /*
     Only major structural liquidity
     participates in true sweeps.
  */

  const levels =
    structure.levels.filter(
      isMajorLevel
    );

  for (
    const level of levels
  ) {

    /*
       Penetration amount.
    */

    const penetration =
      level.price *
      SETTINGS.sweepMinPenetrationPct;

    /*
       =========================================
       BSL SWEEP
       =========================================

       Price trades ABOVE the entire
       structural zone.

       Then candle closes BACK BELOW
       the zone.

       This is a true wick/reclaim
       definition rather than merely
       crossing the level.
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
       =========================================
       SSL SWEEP
       =========================================

       Price trades BELOW the entire
       structural zone.

       Then candle closes BACK ABOVE
       the zone.
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

  /*
     Retain only recent sweeps.
  */

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

  /*
     Add event.
  */

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
   PUBLIC STRUCTURE
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

function publicPersistence(side) {

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

    .map(item => {

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

        firstSeen:
          new Date(
            item.firstSeen
          ).toISOString(),

        lastSeen:
          new Date(
            item.lastSeen
          ).toISOString(),

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

        structuralTimeframes:
          structuralLevel
            ? structuralLevel.timeframes
            : []
      };
    });
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

  /*
     Major structural liquidity
     above and below price.
  */

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

  /*
     Tactical nearby levels.
  */

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

  } else if (bsl.length) {

    status =
      'MAJOR UPSIDE LIQUIDITY ONLY';

  } else if (ssl.length) {

    status =
      'MAJOR DOWNSIDE LIQUIDITY ONLY';
  }

  return {

    state: status,

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
        .map(publicLevel),

    nextSSL:
      ssl
        .slice(0, 5)
        .map(publicLevel),

    nearBSL:
      nearBsl
        .slice(0, 5)
        .map(publicLevel),

    nearSSL:
      nearSsl
        .slice(0, 5)
        .map(publicLevel),

    /*
       CURRENT order-book clusters.
       These are NOT automatically
       structural levels.
    */

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
                cluster.levels
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
                cluster.levels
            })
          )
    },

    /*
       PERSISTENT liquidity.
    */

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

    /*
       Liquidity events.
    */

    liquidityEvents:
      state.liquidityEvents
        .slice(0, 20),

    /*
       True sweep events.
    */

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
      bsl.map(publicLevel),

    sellSideLiquidity:
      ssl.map(publicLevel),

    majorResistance:
      bsl
        .slice(0, 5)
        .map(publicLevel),

    majorSupport:
      ssl
        .slice(0, 5)
        .map(publicLevel),

    nearestBSL:
      bsl[0]
        ? publicLevel(bsl[0])
        : null,

    nearestSSL:
      ssl[0]
        ? publicLevel(ssl[0])
        : null,

    nearBSL:
      nearStructural('BSL')
        .map(publicLevel),

    nearSSL:
      nearStructural('SSL')
        .map(publicLevel)
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
   SNAPSHOT BOOK
============================================================ */

function snapshotBookApply(
  payload
) {

  book.bids.clear();
  book.asks.clear();

  for (
    const [priceString, qtyString]
    of payload.bids || []
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(qtyString);

    if (qty > 0) {

      book.bids.set(
        price,
        qty
      );
    }
  }

  for (
    const [priceString, qtyString]
    of payload.asks || []
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(qtyString);

    if (qty > 0) {

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
   DEPTH EVENTS
============================================================ */

function applyDepthEvent(e) {

  const U =
    Number(e.U);

  const u =
    Number(e.u);

  /*
     Buffer until snapshot exists.
  */

  if (
    !state.bookInitialized
  ) {

    state.pendingEvents.push(e);

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

    state.pendingEvents.push(e);

    requestSnapshot(
      'sequence_gap'
    );

    return;
  }

  /*
     Apply bids.
  */

  for (
    const [priceString, qtyString]
    of e.b || []
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(qtyString);

    if (qty === 0) {

      book.bids.delete(price);

    } else {

      book.bids.set(
        price,
        qty
      );
    }
  }

  /*
     Apply asks.
  */

  for (
    const [priceString, qtyString]
    of e.a || []
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(qtyString);

    if (qty === 0) {

      book.asks.delete(price);

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

  if (index < 0) {

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
      const [priceString, qtyString]
      of event.b || []
    ) {

      const price =
        Number(priceString);

      const qty =
        Number(qtyString);

      if (qty === 0) {

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
      const [priceString, qtyString]
      of event.a || []
    ) {

      const price =
        Number(priceString);

      const qty =
        Number(qtyString);

      if (qty === 0) {

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
      Number(event.u);
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

          method: 'depth',

          params: {
            symbol: SYMBOL,
            limit: 1000
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
          msg.status === 429
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

        clearTimeout(timer);

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

      clearTimeout(timer);

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

      clearTimeout(timer);

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

    arr.push(candle);

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
     Sweep detection is performed
     only on CLOSED candles.
  */

  if (closed) {

    detectCandleSweeps(
      tf,
      candle
    );

    rebuildStructure();
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
          candleFromKline(k);

        upsertCandle(
          tf,
          candle,
          !!k.x
        );

        /*
           Kline price is retained
           as supplemental price data.
           Order-book midpoint will
           normally take priority.
        */

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
   PERSISTENCE / MAINTENANCE LOOP
============================================================ */

setInterval(
  () => {

    /*
       Recover order book if needed.
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

    /*
       Update live price and
       persistence tracker.
    */

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

      rebuildStructure();
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
