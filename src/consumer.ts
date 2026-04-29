import { Kafka, Consumer, Producer, CompressionTypes, logLevel } from "kafkajs";
import { WebSocket } from "ws";

const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";
const WEBSOCKET_URL = process.env.WS_URL || "ws://localhost:3000";
const TOPIC = "realtime-messages";

interface ClientMessage {
  type: "message" | "status" | "stats";
  payload: unknown;
  timestamp: number;
  clientId: string;
}

interface BroadcastMessage {
  type: "kafka" | "system";
  payload: unknown;
  origin: string;
  timestamp: number;
}

type ConnectionStatus = "disconnected" | "connecting" | "connected" | "error";

interface Metrics {
  messagesSent: number;
  messagesReceived: number;
  wsMessagesReceived: number;
  errors: number;
  startTime: number;
  latencyAvg: number;
  latencyMin: number;
  latencyMax: number;
}

class KafkaWebSocketClient {
  private ws: WebSocket | null = null;
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private clientId: string;
  private status: ConnectionStatus = "disconnected";
  private metrics: Metrics;
  private latencies: number[] = [];
  private reconnectAttempts: number = 0;
  private maxReconnectAttempts: number = 10;
  private reconnectDelay: number = 1000;

  constructor() {
    this.clientId = `client-${Date.now()}`;
    this.metrics = {
      messagesSent: 0,
      messagesReceived: 0,
      wsMessagesReceived: 0,
      errors: 0,
      startTime: Date.now(),
      latencyAvg: 0,
      latencyMin: Infinity,
      latencyMax: 0,
    };

    this.kafka = new Kafka({
      clientId: `realtime-demo-${this.clientId}`,
      brokers: [KAFKA_BROKER],
      logLevel: logLevel.WARN,
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
    });

    this.consumer = this.kafka.consumer({
      groupId: `realtime-demo-group-${this.clientId}`,
    });
  }

  async initialize(): Promise<void> {
    console.log("[Client] Initializing Kafka producer and consumer...");

    await this.producer.connect();
    console.log("[Kafka] Producer connected");

    await this.consumer.connect();
    console.log("[Kafka] Consumer connected");

    await this.consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleKafkaMessage(topic, partition, message);
      },
    });

    console.log("[Kafka] Consumer running");

    this.connectWebSocket();
    this.printMetrics();
  }

  private connectWebSocket(): void {
    this.status = "connecting";
    console.log(`[WebSocket] Connecting to ${WEBSOCKET_URL}...`);

    this.ws = new WebSocket(WEBSOCKET_URL);

    this.ws.on("open", () => {
      this.status = "connected";
      this.reconnectAttempts = 0;
      console.log("[WebSocket] Connected");
    });

    this.ws.on("message", (data: Buffer) => {
      this.handleWebSocketMessage(data);
    });

    this.ws.on("close", () => {
      this.status = "disconnected";
      console.log("[WebSocket] Disconnected");
      this.attemptReconnect();
    });

    this.ws.on("error", (error: Error) => {
      this.status = "error";
      this.metrics.errors++;
      console.error("[WebSocket] Error:", error.message);
    });
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error("[WebSocket] Max reconnection attempts reached");
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);

    console.log(
      `[WebSocket] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`
    );

    setTimeout(() => {
      this.connectWebSocket();
    }, delay);
  }

  private handleWebSocketMessage(data: Buffer): void {
    try {
      const message: BroadcastMessage = JSON.parse(data.toString());
      this.metrics.wsMessagesReceived++;

      const latency = Date.now() - message.timestamp;
      this.updateLatencyStats(latency);

      console.log(
        `[WebSocket] Received from ${message.origin}:`,
        JSON.stringify(message.payload)
      );

      this.sendStatus();
    } catch (error) {
      this.metrics.errors++;
      console.error("[WebSocket] Failed to parse message");
    }
  }

  private async handleKafkaMessage(
    topic: string,
    partition: number,
    message: { key?: Buffer; value?: Buffer; timestamp?: string }
  ): Promise<void> {
    if (!message.value) return;

    try {
      const parsed: BroadcastMessage = JSON.parse(message.value.toString());
      this.metrics.messagesReceived++;

      console.log(
        `[Kafka Consumer] Message from partition ${partition}:`,
        parsed.payload
      );

      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(
          JSON.stringify({
            type: "status",
            payload: { received: parsed.payload },
            timestamp: Date.now(),
            clientId: this.clientId,
          })
        );
      }
    } catch (error) {
      this.metrics.errors++;
    }
  }

  async sendMessage(payload: unknown): Promise<void> {
    const startTime = Date.now();
    const message: ClientMessage = {
      type: "message",
      payload,
      timestamp: startTime,
      clientId: this.clientId,
    };

    try {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify(message));
        this.metrics.messagesSent++;
        console.log(`[WebSocket] Sent message:`, payload);
      } else {
        await this.sendDirectToKafka(payload);
      }
    } catch (error) {
      this.metrics.errors++;
      console.error("[Client] Failed to send message:", error);
    }
  }

  async sendDirectToKafka(payload: unknown): Promise<void> {
    const startTime = Date.now();

    await this.producer.send({
      topic: TOPIC,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          key: this.clientId,
          value: JSON.stringify({
            type: "kafka",
            payload,
            origin: this.clientId,
            timestamp: startTime,
          }),
        },
      ],
    });

    this.metrics.messagesSent++;
    console.log(`[Kafka Producer] Sent message:`, payload);
  }

  private updateLatencyStats(latency: number): void {
    this.latencies.push(latency);

    if (this.latencies.length > 100) {
      this.latencies.shift();
    }

    const sum = this.latencies.reduce((a, b) => a + b, 0);
    this.metrics.latencyAvg = Math.round(sum / this.latencies.length);
    this.metrics.latencyMin = Math.min(this.metrics.latencyMin, latency);
    this.metrics.latencyMax = Math.max(this.metrics.latencyMax, latency);
  }

  private sendStatus(): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(
        JSON.stringify({
          type: "stats",
          payload: null,
          timestamp: Date.now(),
          clientId: this.clientId,
        })
      );
    }
  }

  private printMetrics(): void {
    setInterval(() => {
      const uptime = Math.floor((Date.now() - this.metrics.startTime) / 1000);
      const messagesTotal =
        this.metrics.messagesSent +
        this.metrics.messagesReceived +
        this.metrics.wsMessagesReceived;

      console.log(`
[Metrics] Uptime: ${uptime}s | Status: ${this.status}
  Sent: ${this.metrics.messagesSent} | Received: ${this.metrics.messagesReceived}
  WS Received: ${this.metrics.wsMessagesReceived} | Total: ${messagesTotal}
  Latency: avg=${this.metrics.latencyAvg}ms | min=${this.metrics.latencyMin}ms | max=${this.metrics.latencyMax}ms
  Errors: ${this.metrics.errors}
      `.trim());
    }, 5000);
  }

  async shutdown(): Promise<void> {
    console.log("[Client] Shutting down...");

    if (this.ws) {
      this.ws.close();
    }

    await this.consumer.disconnect();
    await this.producer.disconnect();

    console.log("[Client] Shutdown complete");
  }
}

async function runDemo(): Promise<void> {
  const client = new KafkaWebSocketClient();

  const signals: NodeJS.Signals[] = ["SIGINT", "SIGTERM"];
  signals.forEach((signal) => {
    process.on(signal, async () => {
      console.log(`\n[Client] Received ${signal}`);
      await client.shutdown();
      process.exit(0);
    });
  });

  await client.initialize();

  let counter = 0;
  const messageInterval = setInterval(async () => {
    counter++;
    const payload = {
      id: counter,
      text: `Message #${counter}`,
      timestamp: Date.now(),
      data: {
        value: Math.random() * 100,
        label: `Sample ${counter}`,
      },
    };

    await client.sendMessage(payload);
  }, 2000);

  setTimeout(() => {
    clearInterval(messageInterval);
    console.log("[Demo] Sending complete");
  }, 30000);
}

runDemo().catch((error) => {
  console.error("[Demo] Failed:", error);
  process.exit(1);
});