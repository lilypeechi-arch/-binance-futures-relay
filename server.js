const express = require('express');
const WebSocket = require('ws');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 10000;
const SYMBOL = 'BTCUSDT';
const S = SYMBOL.toLowerCase();

const SPOT_KLINES =
  'https://data-api.binance.vision/api/v3/klines';

const DEPTH_WS =
  `wss://fstream.binance.com/ws/${S}@depth@100ms`;

const KLINE_WS =
  `wss://fstream.binance.com/stream?streams=${S}@kline_15m/${S}@kline_1h/${S}@kline_4h/${S}@kline_1d`;

const SNAPSHOT_WS =
  'wss://ws-fapi.binance.com/ws-fapi/v1';

/* =========================================================
   TIMEFRAMES
========================================================= */

const TF = {
  '15m': {
    interval: '15m',
    limit: 300,
    left: 3,
    right: 3,
    weight: 1,
    equal: 0.0015
  },

  '1h': {
    interval: '1h',
    limit: 300,
    left: 4,
    right: 4,
    weight: 2,
    equal: 0.0015
  },

  '4h': {
    interval: '4h',
    limit: 250,
    left: 5,
    right: 5,
    weight: 3,
    equal: 0.0020
  },

  '1d': {
    interval: '1d',
    limit: 180,
    left: 5,
    right: 5,
    weight: 4,
    equal: 0.0025
  }
};

/* =========================================================
   CANDLE STATE
========================================================= */

const candles = {
  '15m': [],
  '1h': [],
  '4h': [],
  '1d': []
};

const candleSource = {
  '15m': 'none',
  '1h': 'none',
  '4h': 'none',
  '1d': 'none'
};

const candleStats = {};

for (const tf of Object.keys(TF)) {
  candleStats[tf] = {
    requests: 0,
    success: 0,
    errors: 0
  };
}

let candleLastStatus = null;
let candleLastError = null;

/* =========================================================
   ORDER BOOK
========================================================= */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

let depthLastUpdateId = 0;
let bookInitialized = false;

let depthConnected = false;
let klineConnected = false;
let snapshotConnected = false;

let depthWs = null;
let klineWs = null;
let snapshotWs = null;

let pending = [];

let gapRecovery = false;
let gapDetected = false;
let gapCount = 0;

let lastGapLocal = null;
let lastGapIncoming = null;

/* =========================================================
   SNAPSHOT
========================================================= */

let snapshotPending = false;
let snapshotRequests = 0;
let snapshot429s = 0;

let lastSnapshotRequest = 0;
let lastSnapshotResponse = null;

let snapshotLastStatus = null;
let snapshotLastError = null;

const SNAPSHOT_COOLDOWN = 65000;

/* =========================================================
   STRUCTURE
========================================================= */

const structure = {
  swingHighs: [],
  swingLows: [],
  equalHighs: [],
  equalLows: [],
  majorBSL: [],
  majorSSL: [],
  majorResistance: [],
  majorSupport: [],
  updatedAt: null
};

/* =========================================================
   HELPERS
========================================================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function pct(a, b) {
  if (!a || !b) return 999;

  return Math.abs(a - b) / b;
}

function round(value) {
  return Number(Number(value).toFixed(2));
}

function iso() {
  return new Date().toISOString();
}

/* =========================================================
   HISTORICAL SPOT CANDLES
========================================================= */

async function loadHistory(tf) {
  const cfg = TF[tf];

  candleStats[tf].requests++;

  try {
    const response = await axios.get(
      SPOT_KLINES,
      {
        params: {
          symbol: SYMBOL,
          interval: cfg.interval,
          limit: cfg.limit
        },

        timeout: 15000,

        headers: {
          'User-Agent': 'Mozilla/5.0'
        }
      }
    );

    const result = [];

    for (const row of response.data || []) {
      if (!Array.isArray(row) || row.length < 6) {
        continue;
      }

      result.push({
        openTime: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5]),
        closeTime: Number(row[6] || 0),
        source: 'spot'
      });
    }

    if (!result.length) {
      throw new Error('No candles returned');
    }

    candles[tf] = result;

    candleSource[tf] =
      'binance_spot';

    candleStats[tf].success++;

    candleLastStatus =
      response.status;

    candleLastError = null;

    console.log(
      `[HISTORY] ${tf}: ${result.length} candles`
    );

    return true;

  } catch (error) {

    candleStats[tf].errors++;

    candleLastStatus =
      error.response?.status || null;

    candleLastError =
      error.response?.data
        ? JSON.stringify(error.response.data)
        : error.message;

    console.log(
      `[HISTORY ERROR] ${tf}: ${candleLastStatus} ${candleLastError}`
    );

    return false;
  }
}

/* =========================================================
   LOAD ALL HISTORY
========================================================= */

async function loadAllHistory() {

  console.log(
    '[HISTORY] Loading Binance Spot candles...'
  );

  for (const tf of Object.keys(TF)) {

    await loadHistory(tf);

    await sleep(250);
  }

  rebuildStructure();

  console.log(
    '[HISTORY] Historical structure initialized'
  );
}

/* =========================================================
   PIVOTS
========================================================= */

function isPivotHigh(
  list,
  index,
  left,
  right
) {

  if (
    index < left ||
    index + right >= list.length
  ) {
    return false;
  }

  const value =
    list[index].high;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) {
      continue;
    }

    if (
      list[i].high >= value
    ) {
      return false;
    }
  }

  return true;
}

function isPivotLow(
  list,
  index,
  left,
  right
) {

  if (
    index < left ||
    index + right >= list.length
  ) {
    return false;
  }

  const value =
    list[index].low;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) {
      continue;
    }

    if (
      list[i].low <= value
    ) {
      return false;
    }
  }

  return true;
}

/* =========================================================
   SWINGS
========================================================= */

function swings(tf) {

  const cfg = TF[tf];

  const list =
    candles[tf];

  const highs = [];
  const lows = [];

  for (
    let i = cfg.left;
    i < list.length - cfg.right;
    i++
  ) {

    if (
      isPivotHigh(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      highs.push({
        timeframe: tf,
        price: list[i].high,
        time: list[i].openTime
      });
    }

    if (
      isPivotLow(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      lows.push({
        timeframe: tf,
        price: list[i].low,
        time: list[i].openTime
      });
    }
  }

  return {
    highs,
    lows
  };
}

/* =========================================================
   EQUAL LEVELS
========================================================= */

function equalLevels(
  levels,
  tolerance
) {

  const groups = [];

  for (const level of levels) {

    let group =
      groups.find(
        item =>
          pct(
            level.price,
            item.price
          ) <= tolerance
      );

    if (!group) {

      group = {
        price: level.price,
        count: 0,
        timeframes: new Set()
      };

      groups.push(group);
    }

    group.price =
      (
        group.price * group.count +
        level.price
      ) /
      (group.count + 1);

    group.count++;

    group.timeframes.add(
      level.timeframe
    );
  }

  return groups
    .filter(
      group =>
        group.count >= 2
    )
    .map(
      group => ({
        price: group.price,
        count: group.count,
        timeframes:
          [...group.timeframes]
      })
    );
}

/* =========================================================
   MERGE NEARBY STRUCTURAL LEVELS
========================================================= */

function merge(
  levels,
  side
) {

  const groups = [];

  const sorted =
    [...levels].sort(
      (a, b) =>
        a.price - b.price
    );

  for (const level of sorted) {

    let group =
      groups.find(
        item =>
          Math.abs(
            item.price -
            level.price
          ) <= 35
      );

    if (!group) {

      group = {
        side,
        prices: [],
        timeframes: new Set(),
        price: level.price
      };

      groups.push(group);
    }

    group.prices.push(
      level.price
    );

    group.timeframes.add(
      level.timeframe
    );

    group.price =
      group.prices.reduce(
        (a, b) => a + b,
        0
      ) /
      group.prices.length;
  }

  return groups.map(
    group => ({
      side: group.side,

      price: group.price,

      priceLow:
        Math.min(
          ...group.prices
        ),

      priceHigh:
        Math.max(
          ...group.prices
        ),

      touches:
        group.prices.length,

      timeframes:
        [...group.timeframes]
    })
  );
}

/* =========================================================
   REACTION COUNT
========================================================= */

function reactionCount(
  price,
  tf
) {

  const list =
    candles[tf];

  let count = 0;

  const zone =
    price * 0.0015;

  for (
    let i = 0;
    i < list.length - 4;
    i++
  ) {

    if (
      list[i].high <
        price - zone ||
      list[i].low >
        price + zone
    ) {
      continue;
    }

    let high =
      list[i].high;

    let low =
      list[i].low;

    for (
      let j = i + 1;
      j <= i + 4;
      j++
    ) {

      high =
        Math.max(
          high,
          list[j].high
        );

      low =
        Math.min(
          low,
          list[j].low
        );
    }

    if (
      (high - price) / price >=
        0.003 ||
      (price - low) / price >=
        0.003
    ) {

      count++;
    }
  }

  return count;
}

/* =========================================================
   RESTING LIQUIDITY
========================================================= */

function restingNear(
  price,
  side
) {

  const book =
    side === 'BSL'
      ? asks
      : bids;

  const levels = [];

  let totalUsd = 0;

  for (
    const [
      priceText,
      quantity
    ] of book
  ) {

    const levelPrice =
      Number(priceText);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(levelPrice) ||
      !Number.isFinite(qty)
    ) {
      continue;
    }

    if (
      Math.abs(
        levelPrice - price
      ) > 35
    ) {
      continue;
    }

    const usd =
      levelPrice * qty;

    if (usd < 50000) {
      continue;
    }

    totalUsd += usd;

    levels.push({
      price: levelPrice,
      quantity: qty,
      usd
    });
  }

  levels.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    totalUsd,
    levels:
      levels.slice(0, 10)
  };
}

/* =========================================================
   STRUCTURAL SCORE
========================================================= */

function score(
  level,
  side,
  equal
) {

  let scoreValue = 0;

  for (
    const tf of level.timeframes
  ) {

    scoreValue +=
      (
        TF[tf]?.weight || 0
      ) * 10;
  }

  if (
    level.timeframes.length >= 2
  ) {

    scoreValue += 10;
  }

  if (
    level.timeframes.length >= 3
  ) {

    scoreValue += 10;
  }

  scoreValue +=
    Math.min(
      level.touches * 3,
      15
    );

  let reactions = 0;

  for (
    const tf of level.timeframes
  ) {

    reactions +=
      reactionCount(
        level.price,
        tf
      );
  }

  scoreValue +=
    Math.min(
      reactions * 2,
      15
    );

  let equalCount = 0;

  for (
    const item of equal
  ) {

    if (
      pct(
        item.price,
        level.price
      ) <= 0.0015
    ) {

      equalCount +=
        item.count;
    }
  }

  scoreValue +=
    Math.min(
      equalCount * 6,
      18
    );

  const resting =
    restingNear(
      level.price,
      side
    );

  if (
    resting.totalUsd >=
    250000
  ) {

    scoreValue += 15;

  } else if (
    resting.totalUsd >=
    100000
  ) {

    scoreValue += 7;
  }

  return {
    ...level,

    score:
      Math.round(
        scoreValue
      ),

    reactions,

    equalCount,

    restingUsd:
      resting.totalUsd,

    restingLevels:
      resting.levels,

    source:
      'spot_structure+futures_liquidity'
  };
}

/* =========================================================
   REBUILD STRUCTURE
========================================================= */

function rebuildStructure() {

  const highs = [];
  const lows = [];

  const equalHighs = [];
  const equalLows = [];

  for (
    const tf of Object.keys(TF)
  ) {

    const result =
      swings(tf);

    highs.push(
      ...result.highs
    );

    lows.push(
      ...result.lows
    );

    equalHighs.push(
      ...equalLevels(
        result.highs,
        TF[tf].equal
      )
    );

    equalLows.push(
      ...equalLevels(
        result.lows,
        TF[tf].equal
      )
    );
  }

  structure.swingHighs =
    highs;

  structure.swingLows =
    lows;

  structure.equalHighs =
    equalHighs;

  structure.equalLows =
    equalLows;

  structure.majorBSL =
    merge(
      highs,
      'BSL'
    )
      .map(
        level =>
          score(
            level,
            'BSL',
            equalHighs
          )
      )
      .filter(
        level =>
          level.score >= 40
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        8
      );

  structure.majorSSL =
    merge(
      lows,
      'SSL'
    )
      .map(
        level =>
          score(
            level,
            'SSL',
            equalLows
          )
      )
      .filter(
        level =>
          level.score >= 40
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        8
      );

  structure.majorResistance =
    structure.majorBSL
      .filter(
        level =>
          currentPrice === null ||
          level.price >
            currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(
        0,
        3
      );

  structure.majorSupport =
    structure.majorSSL
      .filter(
        level =>
          currentPrice === null ||
          level.price <
            currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(
        0,
        3
      );

  structure.updatedAt =
    iso();
}

/* =========================================================
   ORDER BOOK UPDATE
========================================================= */

function applyDepth(data) {

  const U =
    Number(data.U);

  const u =
    Number(data.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  if (!bookInitialized) {

    pending.push(data);

    if (
      pending.length > 20000
    ) {

      pending =
        pending.slice(
          -20000
        );
    }

    return;
  }

  const expected =
    depthLastUpdateId + 1;

  if (
    U > expected
  ) {

    lastGapLocal =
      depthLastUpdateId;

    lastGapIncoming =
      U;

    gapDetected = true;

    if (!gapRecovery) {

      gapRecovery = true;

      gapCount++;

      console.log(
        '[DEPTH] GAP',
        expected,
        U
      );
    }

    return;
  }

  if (
    u <= depthLastUpdateId
  ) {
    return;
  }

  for (
    const item of data.b || []
  ) {

    if (
      Number(item[1]) === 0
    ) {

      bids.delete(
        item[0]
      );

    } else {

      bids.set(
        item[0],
        Number(item[1])
      );
    }
  }

  for (
    const item of data.a || []
  ) {

    if (
      Number(item[1]) === 0
    ) {

      asks.delete(
        item[0]
      );

    } else {

      asks.set(
        item[0],
        Number(item[1])
      );
    }
  }

  depthLastUpdateId =
    u;

  updatePrice();
}

/* =========================================================
   CURRENT PRICE
========================================================= */

function updatePrice() {

  let bestBid = null;
  let bestAsk = null;

  for (
    const priceText of bids.keys()
  ) {

    const price =
      Number(priceText);

    if (
      bestBid === null ||
      price > bestBid
    ) {

      bestBid = price;
    }
  }

  for (
    const priceText of asks.keys()
  ) {

    const price =
      Number(priceText);

    if (
      bestAsk === null ||
      price < bestAsk
    ) {

      bestAsk = price;
    }
  }

  if (
    bestBid !== null &&
    bestAsk !== null
  ) {

    currentPrice =
      (bestBid + bestAsk) / 2;
  }
}

/* =========================================================
   SNAPSHOT
========================================================= */

function requestSnapshot() {

  const timestamp =
    Date.now();

  if (snapshotPending) {
    return;
  }

  if (
    timestamp -
      lastSnapshotRequest <
    SNAPSHOT_COOLDOWN
  ) {
    return;
  }

  lastSnapshotRequest =
    timestamp;

  snapshotPending =
    true;

  snapshotRequests++;

  console.log(
    '[SNAPSHOT] Requesting...'
  );

  try {

    if (snapshotWs) {

      try {
        snapshotWs.close();
      } catch (_) {}
    }

    snapshotWs =
      new WebSocket(
        SNAPSHOT_WS
      );

    snapshotWs.on(
      'open',
      () => {

        snapshotConnected =
          true;

        snapshotWs.send(
          JSON.stringify({
            id:
              String(
                Date.now()
              ),

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

    snapshotWs.on(
      'message',
      raw => {

        try {

          const message =
            JSON.parse(
              raw.toString()
            );

          if (
            message.status &&
            message.status !== 200
          ) {

            snapshotLastStatus =
              message.status;

            snapshotLastError =
              JSON.stringify(
                message.error ||
                message
              );

            if (
              message.status ===
              429
            ) {

              snapshot429s++;
            }

            snapshotPending =
              false;

            return;
          }

          if (
            !message.result ||
            !message.result.lastUpdateId
          ) {

            return;
          }

          const result =
            message.result;

          bids.clear();
          asks.clear();

          for (
            const item of
              result.bids || []
          ) {

            if (
              Number(item[1]) > 0
            ) {

              bids.set(
                item[0],
                Number(item[1])
              );
            }
          }

          for (
            const item of
              result.asks || []
          ) {

            if (
              Number(item[1]) > 0
            ) {

              asks.set(
                item[0],
                Number(item[1])
              );
            }
          }

          depthLastUpdateId =
            Number(
              result.lastUpdateId
            );

          bookInitialized =
            true;

          const buffered =
            [...pending].sort(
              (a, b) =>
                Number(a.U) -
                Number(b.U)
            );

          pending = [];

          const target =
            depthLastUpdateId + 1;

          const bridge =
            buffered.findIndex(
              item =>
                Number(item.U) <=
                  target &&
                Number(item.u) >=
                  target
            );

          if (
            bridge >= 0
          ) {

            for (
              let i = bridge;
              i < buffered.length;
              i++
            ) {

              applyDepth(
                buffered[i]
              );
            }

            gapRecovery =
              false;

            gapDetected =
              false;

          } else if (
            buffered.length
          ) {

            gapRecovery =
              true;

            gapDetected =
              true;

          } else {

            gapRecovery =
              false;

            gapDetected =
              false;
          }

          updatePrice();

          snapshotLastStatus =
            200;

          snapshotLastError =
            null;

          lastSnapshotResponse =
            iso();

          snapshotPending =
            false;

          console.log(
            `[SNAPSHOT] Installed ${depthLastUpdateId} bids=${bids.size} asks=${asks.size}`
          );

        } catch (error) {

          snapshotLastError =
            error.message;

          snapshotPending =
            false;
        }
      }
    );

    snapshotWs.on(
      'error',
      error => {

        snapshotLastError =
          error.message;

        snapshotPending =
          false;

        console.log(
          '[SNAPSHOT ERROR]',
          error.message
        );
      }
    );

    snapshotWs.on(
      'close',
      () => {

        snapshotConnected =
          false;
      }
    );

  } catch (error) {

    snapshotLastError =
      error.message;

    snapshotPending =
      false;
  }
}

/* =========================================================
   DEPTH WEBSOCKET
========================================================= */

function connectDepth() {

  if (depthWs) {

    try {
      depthWs.close();
    } catch (_) {}
  }

  depthWs =
    new WebSocket(
      DEPTH_WS
    );

  depthWs.on(
    'open',
    () => {

      depthConnected =
        true;

      console.log(
        '[DEPTH] Connected'
      );

      requestSnapshot();
    }
  );

  depthWs.on(
    'message',
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );

        if (
          data.e ===
          'depthUpdate'
        ) {

          applyDepth(
            data
          );
        }

      } catch (error) {

        console.log(
          '[DEPTH MESSAGE ERROR]',
          error.message
        );
      }
    }
  );

  depthWs.on(
    'error',
    error => {

      console.log(
        '[DEPTH ERROR]',
        error.message
      );
    }
  );

  depthWs.on(
    'close',
    () => {

      depthConnected =
        false;

      console.log(
        '[DEPTH] Disconnected'
      );

      setTimeout(
        connectDepth,
        5000
      );
    }
  );
}

/* =========================================================
   FUTURES KLINES
========================================================= */

function connectKlines() {

  if (klineWs) {

    try {
      klineWs.close();
    } catch (_) {}
  }

  klineWs =
    new WebSocket(
      KLINE_WS
    );

  klineWs.on(
    'open',
    () => {

      klineConnected =
        true;

      console.log(
        '[KLINE] Connected'
      );
    }
  );

  klineWs.on(
    'message',
    raw => {

      try {

        const message =
          JSON.parse(
            raw.toString()
          );

        const data =
          message.data ||
          message;

        if (
          data.e !==
          'kline' ||
          !data.k
        ) {

          return;
        }

        const k =
          data.k;

        const tf =
          k.i;

        if (
          !TF[tf]
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
            Number(k.T || 0),

          source:
            'futures'
        };

        const list =
          candles[tf];

        const index =
          list.findIndex(
            item =>
              item.openTime ===
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
          TF[tf].limit + 50;

        if (
          list.length > max
        ) {

          candles[tf] =
            list.slice(
              -max
            );
        }

        candleSource[tf] =
          'binance_futures_live';

        currentPrice =
          candle.close;

        if (
          k.x === true
        ) {

          rebuildStructure();
        }

      } catch (error) {

        console.log(
          '[KLINE ERROR]',
          error.message
        );
      }
    }
  );

  klineWs.on(
    'error',
    error => {

      console.log(
        '[KLINE ERROR]',
        error.message
      );
    }
  );

  klineWs.on(
    'close',
    () => {

      klineConnected =
        false;

      console.log(
        '[KLINE] Disconnected'
      );

      setTimeout(
        connectKlines,
        5000
      );
    }
  );
}

/* =========================================================
   RAW ORDER BOOK
========================================================= */

function rawBook() {

  const largeBids = [];
  const largeAsks = [];

  for (
    const [
      priceText,
      quantity
    ] of bids
  ) {

    const price =
      Number(priceText);

    const usd =
      price *
      quantity;

    if (
      usd >= 50000
    ) {

      largeBids.push({
        price,
        quantity,
        usd
      });
    }
  }

  for (
    const [
      priceText,
      quantity
    ] of asks
  ) {

    const price =
      Number(priceText);

    const usd =
      price *
      quantity;

    if (
      usd >= 50000
    ) {

      largeAsks.push({
        price,
        quantity,
        usd
      });
    }
  }

  largeBids.sort(
    (a, b) =>
      b.usd - a.usd
  );

  largeAsks.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    largeBids:
      largeBids.slice(
        0,
        25
      ),

    largeAsks:
      largeAsks.slice(
        0,
        25
      )
  };
}

/* =========================================================
   FORMAT STRUCTURAL LEVEL
========================================================= */

function formatLevel(level) {

  return {

    side:
      level.side,

    price:
      round(level.price),

    priceLow:
      round(level.priceLow),

    priceHigh:
      round(level.priceHigh),

    score:
      level.score,

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      round(level.restingUsd),

    restingLevels:
      level.restingLevels,

    distancePct:
      currentPrice === null
        ? null
        : Number(
            (
              (
                level.price -
                currentPrice
              ) /
              currentPrice
            * 100
            ).toFixed(3)
          ),

    source:
      level.source
  };
}

/* =========================================================
   LIQUIDITY MAP
========================================================= */

function getLiquidityMap() {

  const bsl =
    structure.majorBSL
      .filter(
        level =>
          currentPrice === null ||
          level.price >
            currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const ssl =
    structure.majorSSL
      .filter(
        level =>
          currentPrice === null ||
          level.price <
            currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      );

  return {

    buySideLiquidity:
      bsl.map(
        formatLevel
      ),

    sellSideLiquidity:
      ssl.map(
        formatLevel
      ),

    majorResistance:
      bsl
        .slice(0, 3)
        .map(
          formatLevel
        ),

    majorSupport:
      ssl
        .slice(0, 3)
        .map(
          formatLevel
        ),

    nearestBSL:
      bsl.length
        ? [
            formatLevel(
              bsl[0]
            )
          ]
        : [],

    nearestSSL:
      ssl.length
        ? [
            formatLevel(
              ssl[0]
            )
          ]
        : []
  };
}

/* =========================================================
   HEALTH
========================================================= */

app.get(
  '/health',
  (req, res) => {

    res.json({

      ok:
        true,

      service:
        'Binance BTCUSDT Major Liquidity Relay',

      symbol:
        SYMBOL,

      currentPrice,

      historicalStructureSource:
        'Binance Spot',

      liveStructureSource:
        'Binance Futures WebSocket',

      liquiditySource:
        'Binance Futures Order Book',

      candleHistory:
        Object.fromEntries(
          Object.keys(TF)
            .map(
              tf => [
                tf,
                candles[tf].length
              ]
            )
        ),

      candleSource,

      candleStats,

      candleLastStatus,

      candleLastError,

      structure: {

        swingHighs:
          structure.swingHighs.length,

        swingLows:
          structure.swingLows.length,

        equalHighs:
          structure.equalHighs.length,

        equalLows:
          structure.equalLows.length,

        majorBSL:
          structure.majorBSL.length,

        majorSSL:
          structure.majorSSL.length,

        updatedAt:
          structure.updatedAt
      },

      orderBook: {

        initialized:
          bookInitialized,

        bids:
          bids.size,

        asks:
          asks.size,

        lastUpdateId:
          depthLastUpdateId
      },

      connections: {

        depth:
          depthConnected,

        snapshot:
          snapshotConnected,

        klines:
          klineConnected
      },

      sync: {

        gapRecovery,

        gapDetected,

        gapCount,

        lastGapLocal,

        lastGapIncoming,

        pendingEvents:
          pending.length,

        snapshotPending,

        snapshotId:
          depthLastUpdateId,

        snapshotRequests,

        snapshot429s,

        lastSnapshotResponse,

        snapshotLastStatus,

        snapshotLastError
      },

      updatedAt:
        iso()
    });
  }
);

/* =========================================================
   STRUCTURE ENDPOINT
========================================================= */

app.get(
  '/structure',
  (req, res) => {

    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      currentPrice,

      historicalSource:
        'Binance Spot',

      liveSource:
        'Binance Futures',

      swingHighs:
        structure.swingHighs,

      swingLows:
        structure.swingLows,

      equalHighs:
        structure.equalHighs,

      equalLows:
        structure.equalLows,

      majorBSL:
        structure.majorBSL.map(
          formatLevel
        ),

      majorSSL:
        structure.majorSSL.map(
          formatLevel
        ),

      majorResistance:
        structure.majorResistance.map(
          formatLevel
        ),

      majorSupport:
        structure.majorSupport.map(
          formatLevel
        ),

      updatedAt:
        structure.updatedAt
    });
  }
);

/* =========================================================
   LIQUIDITY ENDPOINT
========================================================= */

app.get(
  '/liquidity',
  (req, res) => {

    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      currentPrice,

      marketStructure:
        getLiquidityMap(),

      rawRestingLiquidity:
        rawBook(),

      structureStats: {

        candleSource,

        candleHistory:
          Object.fromEntries(
            Object.keys(TF)
              .map(
                tf => [
                  tf,
                  candles[tf].length
                ]
              )
          ),

        swingHighs:
          structure.swingHighs.length,

        swingLows:
          structure.swingLows.length,

        equalHighs:
          structure.equalHighs.length,

        equalLows:
          structure.equalLows.length
      },

      updatedAt:
        iso()
    });
  }
);

/* =========================================================
   BOOK ENDPOINT
========================================================= */

app.get(
  '/book',
  (req, res) => {

    const bidList =
      [...bids.entries()]
        .map(
          ([price, quantity]) => ({
            price:
              Number(price),

            quantity,

            usd:
              Number(price) *
              quantity
          })
        )
        .sort(
          (a, b) =>
            b.price - a.price
        )
        .slice(
          0,
          1000
        );

    const askList =
      [...asks.entries()]
        .map(
          ([price, quantity]) => ({
            price:
              Number(price),

            quantity,

            usd:
              Number(price) *
              quantity
          })
        )
        .sort(
          (a, b) =>
            a.price - b.price
        )
        .slice(
          0,
          1000
        );

    res.json({

      ok:
        true,

      symbol:
        SYMBOL,

      currentPrice,

      initialized:
        bookInitialized,

      lastUpdateId:
        depthLastUpdateId,

      bids:
        bidList,

      asks:
        askList,

      updatedAt:
        iso()
    });
  }
);

/* =========================================================
   ROOT
========================================================= */

app.get(
  '/',
  (req, res) => {

    res.json({

      ok:
        true,

      service:
        'Binance BTCUSDT Major Liquidity Relay',

      symbol:
        SYMBOL,

      endpoints: [
        '/health',
        '/structure',
        '/liquidity',
        '/book'
      ]
    });
  }
);

/* =========================================================
   RECOVERY
========================================================= */

setInterval(
  () => {

    if (gapRecovery) {
      requestSnapshot();
    }

  },
  5000
);

/* =========================================================
   STRUCTURE REFRESH
========================================================= */

setInterval(
  () => {

    rebuildStructure();

  },
  30000
);

/* =========================================================
   START
========================================================= */

app.listen(
  PORT,
  async () => {

    console.log(
      `Server listening on port ${PORT}`
    );

    console.log(
      '[START] Loading historical Spot candles'
    );

    await loadAllHistory();

    console.log(
      '[START] Connecting Futures depth'
    );

    connectDepth();

    console.log(
      '[START] Connecting Futures klines'
    );

    connectKlines();
  }
);
