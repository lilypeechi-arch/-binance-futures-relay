const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "btcusdt";

let latestBook = {
  symbol: "BTCUSDT",
  bids: [],
  asks: [],
  updatedAt: null,
  connected: false
};

let ws = null;
let reconnectTimer = null;

function connectBinance() {
  console.log("Connecting to Binance Futures WebSocket...");

  ws = new WebSocket(
    `wss://fstream.binance.com/ws/${SYMBOL}@depth20@100ms`
  );

  ws.on("open", () => {
    console.log("Connected to Binance Futures WebSocket.");
    latestBook.connected = true;
  });

  ws.on("message", (data) => {
    try {
      const message = JSON.parse(data.toString());

      if (message.b && message.a) {
        latestBook = {
          symbol: "BTCUSDT",
          bids: message.b.map(x => ({
            price: Number(x[0]),
            quantity: Number(x[1]),
            usd: Number(x[0]) * Number(x[1])
          })),
          asks: message.a.map(x => ({
            price: Number(x[0]),
            quantity: Number(x[1]),
            usd: Number(x[0]) * Number(x[1])
          })),
          updatedAt: new Date().toISOString(),
          connected: true
        };
      }
    } catch (error) {
      console.log("Message parse error:", error.message);
    }
  });

  ws.on("close", () => {
    console.log("Binance WebSocket disconnected.");

    latestBook.connected = false;

    scheduleReconnect();
  });

  ws.on("error", (error) => {
    console.log("Binance WebSocket error:", error.message);
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectBinance();
  }, 5000);
}

app.get("/", (req, res) => {
  res.json({
    service: "Binance Futures Liquidity Relay",
    status: "online",
    binanceConnected: latestBook.connected,
    updatedAt: latestBook.updatedAt
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    binanceConnected: latestBook.connected,
    updatedAt: latestBook.updatedAt
  });
});

app.get("/book", (req, res) => {
  res.json(latestBook);
});

app.listen(PORT, () => {
  console.log(`Relay server running on port ${PORT}`);
  connectBinance();
});
