import { WebSocket } from "ws";

const ws = new WebSocket("ws://localhost:3000");

ws.on("open", () => {
  console.log("[Test] Connected to WebSocket");

  const message = {
    type: "message",
    payload: { text: "Test message from CLI", id: Date.now() },
    timestamp: Date.now(),
    clientId: "test-client",
  };

  ws.send(JSON.stringify(message));
  console.log("[Test] Sent message:", message.payload);
});

ws.on("message", (data) => {
  const msg = JSON.parse(data.toString());
  console.log("[Test] Received:", msg);
});

ws.on("error", (err) => {
  console.error("[Test] Error:", err.message);
});

ws.on("close", () => {
  console.log("[Test] Connection closed");
});
