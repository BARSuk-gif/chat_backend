import { randomUUID } from "node:crypto";
import http from "node:http";
import cors from "cors";
import express from "express";
import pino from "pino";
import pinoPretty from "pino-pretty";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const logger = pino(pinoPretty());

app.use(cors());
app.use(express.json({ type: () => true }));
app.use((req, res, next) => {
  res.setHeader("Content-Type", "application/json");
  next();
});

// ---------- Хранилище ----------
const userState = [];  // [{ id, name }]

function findUserById(id) {
  return userState.find((u) => u.id === id);
}
function removeUserById (id) {
  const idx = userState.findIndex((u) => u.id === id);
  if (idx !== -1) userState.splice(idx, 1);
}

// ---------- HTTP: регистрация ----------
app.post("/new-user", async (req, res) => {
  const name = req.body?.name?.trim();
  if (!name) {
    return res.status(400).json({ status: "error", message: "Name is required" });   
  }
  if (userState.some((u) => u.name === name)) {
    return res.status(409).json({ status: "error", message: "Name is already taken" });
  }
  const user = { id: randomUUID(), name };
  userState.push(user);
  logger.info({ user }, "user created");
  res.json({ status: "ok", user });
});

const server = http.createServer(app);
const wsServer = new WebSocketServer({ server });

function sendTo(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}
function broadcast(payload, { except } = {}) {
  const data = JSON.stringify(payload);
  for (const ws of wsServer.clients) {
    if (ws === except) continue;
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}
function broadcastUsers() {
  broadcast({ type: "users", users: userState });
}

// ---------- WS: подключение ----------
wsServer.on("connection", (ws) => {
  ws.isAlive = true;
  ws.user = null;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw, isBinary) => {
    // 1. Парсинг
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return sendTo(ws, { type: "error", message: "Invalid JSON" });
    }
    if (typeof data?.type !== "string") {
      return sendTo(ws, { type: "error", message: "Missing 'type'" });
    }

    // 2. Роутинг по типу
    switch (data.type) {
      case "auth": {
        const user = findUserById(data.userId);
        if (!user) {
          return sendTo(ws, { type: "error", message: "Unknown user" });
        }
        ws.user = user;
        logger.info({ user }, "ws authenticated");
        sendTo(ws, { type: "auth-ok", user });
        broadcastUsers();
        break;
      }

      case "send": {
        if (!ws.user) {
          return sendTo(ws, { type: "error", message: "Not authenticated" });
        }
        const text = String(data.text ?? "").slice(0, 2000); // лимит
        if (!text) return;
        broadcast(
          { type: "message", from: ws.user, text, ts: Date.now() },
          { except: ws } // не отправляем обратно автору
        );
        logger.info({ from: ws.user.name, text }, "message");
        break;
      }

      case "exit": {
        if (ws.user) {
          removeUserById(ws.user.id);
          logger.info({ user: ws.user }, "user exited");
          ws.user = null;
          broadcastUsers();
        }
        ws.close(1000, "bye");
        break;
      }

      default:
        sendTo(ws, { type: "error", message: `Unknown type: ${data.type}` });
    }
  });

  // Уборка при обрыве
  ws.on("close", () => {
    if (ws.user) {
      removeUserById(ws.user.id);
      logger.info({ user: ws.user }, "ws closed, user removed");
      broadcastUsers();
    }
  });

  ws.on("error", (err) => {
    logger.error({ err: err.message }, "ws error");
  });

  // Новому клиенту — сразу текущий список
  sendTo(ws, { type: "users", users: userState });
});

// ---------- Heartbeat ----------
const heartbeatTimer = setInterval(() => {
  for (const ws of wsServer.clients) {
    if (!ws.isAlive) {
      logger.warn("ws heartbeat timeout, terminating");
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

wsServer.on("close", () => clearInterval(heartbeatTimer));

// ---------- Запуск ----------
const port = process.env.PORT || 3000;

server.on("error", (err) => logger.error({ err }, "server error"));
server.listen(port, () =>
  logger.info(`Server started on http://localhost:${port}`)
)
