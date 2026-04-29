import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer, WebSocket } from "ws";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createServer } from "node:http";
import { readFileSync } from "node:fs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9094";
const TOPIC = "realtime-messages";
const PORT = parseInt(process.env.PORT || "3000");
const SERVER_ID = `server-${Date.now()}`;

// Estado global
const clients = new Set<WebSocket>();
let messageCount = 0;
let startTime = Date.now();
let lastCount = 0;
let messageRate = 0;

// Kafka setup
const kafka = new Kafka({
  clientId: "realtime-demo-server",
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 100, retries: 5 },
});

const producer = kafka.producer({ allowAutoTopicCreation: true });
const consumer = kafka.consumer({ groupId: "realtime-demo-group" });
const admin = kafka.admin();

// Funciones auxiliares
function broadcastToAll(data: string) {
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(data);
      } catch (e) {
        clients.delete(client);
      }
    }
  });
}

function getConnectionStats() {
  return {
    activeConnections: clients.size,
    messagesPerSecond: messageRate,
    totalMessagesProcessed: messageCount,
    uptime: Math.floor((Date.now() - startTime) / 1000),
  };
}

function sendStats() {
  const stats = getConnectionStats();
  broadcastToAll(JSON.stringify({ type: "system", payload: { stats }, origin: "system", timestamp: Date.now() }));
}

// Servir archivos estáticos
const publicPath = join(__dirname, "..", "public");

function serveFile(req: any, res: any) {
  let pathname = new URL(req.url, "http://localhost").pathname;
  pathname = pathname === "/" ? "/index.html" : pathname;

  const safePath = join(publicPath, pathname);
  if (!safePath.startsWith(publicPath)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  try {
    const content = readFileSync(safePath);
    const ext = pathname.split(".").pop() || "";
    const types: Record<string, string> = {
      html: "text/html",
      css: "text/css",
      js: "text/javascript",
      json: "application/json",
    };
    res.writeHead(200, { "Content-Type": types[ext] || "text/plain" });
    res.end(content);
  } catch {
    res.writeHead(404);
    res.end("Not Found");
  }
}

// Crear servidor HTTP + WebSocket
const server = createServer(serveFile);

const wss = new WebSocketServer({ server });

wss.on("connection", (ws: WebSocket) => {
  const clientId = `client-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  (ws as any).clientId = clientId;
  clients.add(ws);
  console.log(`[WS] Connected: ${clientId} (total: ${clients.size})`);

  ws.send(JSON.stringify({
    type: "status",
    payload: { clientId, message: "Connected", status: "connected" },
    timestamp: Date.now(),
    clientId,
  }));
  sendStats();

  ws.on("message", async (data: Buffer) => {
    const clientId = (ws as any).clientId;
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === "message") {
        const kafkaMsg = {
          type: "kafka",
          payload: msg.payload,
          origin: clientId,
          timestamp: Date.now(),
        };
        await producer.send({
          topic: TOPIC,
          messages: [{ key: clientId, value: JSON.stringify(kafkaMsg) }],
        });
        messageCount++;
        sendStats();
      }
    } catch (e) {
      console.error("[WS] Message error:", e);
    }
  });

  ws.on("close", () => {
    const clientId = (ws as any).clientId;
    clients.delete(ws);
    console.log(`[WS] Disconnected: ${clientId} (total: ${clients.size})`);
    sendStats();
  });

  ws.on("error", (error) => {
    console.error("[WS] Error:", error);
  });
});

console.log(`[Server] Listening on http://localhost:${PORT} and ws://localhost:${PORT}`);

// Inicialización Kafka
async function initKafka() {
  console.log(`[Kafka] Connecting to ${KAFKA_BROKER}...`);
  await producer.connect();
  await admin.connect();

  const topics = await admin.listTopics();
  if (!topics.includes(TOPIC)) {
    await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }] });
    console.log(`[Kafka] Topic created: ${TOPIC}`);
    await new Promise(r => setTimeout(r, 1000));
  } else {
    console.log(`[Kafka] Topic exists: ${TOPIC}`);
  }

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  consumer.run({
    eachMessage: async ({ partition, message }) => {
      if (!message.value) return;
      try {
        const parsed = JSON.parse(message.value.toString());
        messageCount++;
        console.log(`[Kafka] Received:`, parsed.payload);
        broadcastToAll(JSON.stringify({
          type: "kafka",
          payload: { ...parsed, partition },
          origin: parsed.origin,
          timestamp: parsed.timestamp,
        }));
        sendStats();
      } catch (e) {
        console.error("[Kafka] Parse error:", e);
      }
    },
  });
  console.log("[Kafka] Consumer running");
}

// Estadísticas periódicas
setInterval(() => {
  const current = messageCount;
  const rate = current - lastCount;
  lastCount = current;
  messageRate = rate;
  console.log(`[Stats] Connections: ${clients.size} | Messages: ${current} | Rate: ${rate}/s`);
  sendStats();
}, 5000);

// Iniciar Kafka y manejar cierre
initKafka().catch(e => {
  console.error("[Kafka] Init failed:", e);
});

process.on("SIGINT", async () => {
  console.log("\n[Server] Shutting down...");
  await consumer.disconnect();
  await producer.disconnect();
  await admin.disconnect();
  wss.close();
  server.close();
  process.exit(0);
});

server.listen(PORT);