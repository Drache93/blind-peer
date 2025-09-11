const { serve } = require("@hono/node-server");
const { Hono } = require("hono");
const { html, raw } = require("hono/html");
const { createMiddleware } = require("hono/factory");
const pino = require("pino");
const { HTTPException } = require("hono/http-exception");
const { stream, streamText, streamSSE } = require("hono/streaming");
const idEnc = require("hypercore-id-encoding");
const b4a = require("b4a");
const byteSize = require("tiny-byte-size");
const { recordToStr, streamToStr, coreToInfo } = require("./lib/to_string");

const app = new Hono();
let manager;

const isHtmxRequest = (c) => {
  return c.req.header("HX-Request") === "true";
};

let id = 0;

class SSELogger {
  #stream = null;
  #events = "";

  constructor(stream) {
    this.#stream = stream;
  }

  async #writeLogs(eventType, level, data, meta) {
    this.#events =
      html`<div class="event log-${level}">
        <div class="log-header">
          <span class="log-timestamp">${new Date().toISOString()}</span>
          <span class="log-event-type">${eventType}</span>
          <span class="log-level log-level-${level}"
            >${level.toUpperCase()}</span
          >
        </div>
        <div class="log-message">${data}</div>
        ${meta
          ? html`<pre class="log-meta">${JSON.stringify(meta, null, 2)}</pre>`
          : ""}
      </div>` + this.#events;

    await this.#stream.writeSSE({
      data: this.#events,
      event: "logs",
      id: String(id++),
    });
  }

  info(eventType, data, meta) {
    return this.#writeLogs(eventType, "info", data, meta);
  }
  error(eventType, data, meta) {
    return this.#writeLogs(eventType, "error", data, meta);
  }
  warn(eventType, data, meta) {
    return this.#writeLogs(eventType, "warn", data, meta);
  }
  debug(eventType, data, meta) {
    return this.#writeLogs(eventType, "debug", data, meta);
  }
}

module.exports = function startServer(blindPeer, debug) {
  const logger = pino({
    level: debug ? "debug" : "info",
    name: "blind-peer-server",
  });

  app.get("/cores", async (c) => {
    const cores = await blindPeer.db.find("@blind-peer/cores");

    const coresData = [];

    for await (const core of cores) {
      coresData.push({
        ...core,
        key: b4a.toString(core.key, "hex"),
      });
    }

    if (!isHtmxRequest(c)) {
      return c.json(coresData);
    }

    return c.html(html`
      <div class="cores-container">
        <h2>Active Cores</h2>
        <div class="cores-grid">
          ${coresData.map(
            (core) => html`
              <div
                class="core-card ${core.announce ? "announcing" : "inactive"}"
              >
                <div class="core-header">
                  <div
                    class="core-status ${core.announce
                      ? "status-active"
                      : "status-inactive"}"
                  ></div>
                  <span class="core-priority">Priority: ${core.priority}</span>
                </div>
                <div class="core-key">
                  <label>Key:</label>
                  <span class="key-value">${core.key}</span>
                  <button
                    class="copy-btn"
                    onclick="navigator.clipboard.writeText('${core.key}')"
                  >
                    📋
                  </button>
                </div>
                <div class="core-stats">
                  <div class="stat">
                    <span class="stat-label">Length:</span>
                    <span class="stat-value"
                      >${core.length.toLocaleString()}</span
                    >
                  </div>
                  <div class="stat">
                    <span class="stat-label">Size:</span>
                    <span class="stat-value"
                      >${byteSize(core.bytesAllocated)}</span
                    >
                  </div>
                  <div class="stat">
                    <span class="stat-label">Cleared:</span>
                    <span class="stat-value"
                      >${byteSize(core.bytesCleared)}</span
                    >
                  </div>
                </div>
                <div class="core-timestamps">
                  <div class="timestamp">
                    <span class="timestamp-label">Updated:</span>
                    <span class="timestamp-value"
                      >${new Date(core.updated).toLocaleString()}</span
                    >
                  </div>
                  <div class="timestamp">
                    <span class="timestamp-label">Active:</span>
                    <span class="timestamp-value"
                      >${new Date(core.active).toLocaleString()}</span
                    >
                  </div>
                </div>
                ${core.referrer ??
                html`
                  <div class="core-referrer">
                    <span class="referrer-label">Referrer:</span>
                    <span class="referrer-value">${core.referrer}</span>
                  </div>
                `}
              </div>
            `,
          )}
        </div>
        ${coresData.length === 0 ??
        html`
          <div class="no-cores">
            <p>No cores found</p>
          </div>
        `}
      </div>
    `);
  });

  app.get("/sse", async (c) => {
    return streamSSE(c, async (stream) => {
      const sseLogger = new SSELogger(stream);

      blindPeer.on("add-core", (record, _, stream) => {
        try {
          sseLogger.info(
            "add-core",
            `Record: ${recordToStr(record)}, Stream: ${streamToStr(stream)}`,
          );
        } catch (e) {
          sseLogger.warn(
            "add-core",
            `Invalid add-core request received: ${e.stack}`,
            record,
          );
        }
      });

      blindPeer.on("flush-error", (e) => {
        sseLogger.warn(
          "flush-error",
          `Invalid add-core request received: ${e.stack}`,
        );
      });

      blindPeer.on("downgrade-announce", ({ record, remotePublicKey }) => {
        try {
          sseLogger.info(
            "downgrade-announce",
            `Downgraded announce for peer ${idEnc.normalize(remotePublicKey)} because the peer is not trusted (Original: ${recordToStr(record)})`,
          );
        } catch (e) {
          sseLogger.error(
            "downgrade-announce",
            `Unexpected error while logging downgrade-announce: ${e.stack}`,
          );
        }
      });

      blindPeer.on("announce-core", (core) => {
        sseLogger.info(
          "announce-core",
          `Started announcing core ${coreToInfo(core)}`,
        );
      });
      blindPeer.on("core-downloaded", (core) => {
        sseLogger.info(
          "core-downloaded",
          `Announced core fully downloaded: ${coreToInfo(core)}`,
        );
      });
      blindPeer.on("core-append", (core) => {
        sseLogger.info(
          "core-append",
          `Detected announced-core length update: ${coreToInfo(core)}`,
        );
      });

      blindPeer.on("gc-start", ({ bytesToClear }) => {
        sseLogger.info(
          "gc-start",
          `Starting GC, trying to clear ${byteSize(bytesToClear)} (bytes allocated: ${byteSize(blindPeer.digest.bytesAllocated)} of ${byteSize(blindPeer.maxBytes)})`,
        );
      });
      blindPeer.on("gc-done", ({ bytesCleared }) => {
        sseLogger.info(
          "gc-done",
          `Completed GC, cleared ${byteSize(bytesCleared)} bytes (bytes allocated: ${byteSize(blindPeer.digest.bytesAllocated)} of ${byteSize(blindPeer.maxBytes)})`,
        );
      });
      if (debug) {
        blindPeer.on("core-activity", (core) => {
          sseLogger.info(
            "core-activity",
            `Core activity for ${coreToInfo(core)}`,
          );
        });
      }

      blindPeer.on("invalid-request", (core, err, req, from) => {
        const address = `${from.stream?.rawStream?.remoteHost}:${from.stream?.rawStream?.remotePort}`;
        const remotePubKey = idEnc.normalize(from.stream.remotePublicKey);
        const key = idEnc.normalize(core.key);
        sseLogger.warn(
          "invalid-request",
          `Received invalid request for core ${key} from peer ${remotePubKey} at ${address} (${err.stack})`,
        );
      });

      sseLogger.info("startup", "Listening to Blind Peer logs");

      while (true) {
        await stream.sleep(1000);
      }
    });
  });

  app.get("/blind-peer", async (c) => {
    const localAddress = blindPeer.swarm.dht.localAddress();
    if (!isHtmxRequest(c)) {
      return c.json(localAddress);
    }

    const trustedKeys = [...blindPeer.trustedPubKeys]
      .map((key) => `<li>${key}</li>`)
      .join("");

    return c.html(html`
      <div>
        <h2>Config</h2>
        <h3>Local Address</h3>
        <p>Host: ${localAddress.host}</p>
        <p>Port: ${localAddress.port}</p>
        <p>Address: ${idEnc.normalize(blindPeer.publicKey)}</p>

        <h3>Trusted Keys</h3>
        <ul>
          ${html(trustedKeys)}
        </ul>
      </div>
    `);
  });

  app.get("/", (c) => {
    return c.html(
      html`<html>
        <head>
          <head>
            <script
              src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.7/dist/htmx.min.js"
              integrity="sha384-ZBXiYtYQ6hJ2Y0ZNoYuI+Nq5MqWBr+chMrS/RkXpNzQCApHEhOt2aY8EJgqwHLkJ"
              crossorigin="anonymous"
            ></script>
            <script
              src="https://cdn.jsdelivr.net/npm/htmx-ext-sse@2.2.2/sse.min.js"
              integrity="sha384-BkCCd4DbvhfvRLfThp+5RN2KiB1FBROoAtwZFfceJishUnpFW1eF/aqG14M5TwA2"
              crossorigin="anonymous"
            ></script>
          </head>
          <style>
            body {
              color: rgb(255, 255, 254);
              background-color: rgb(29, 29, 29);
              font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
              margin: 0;
              padding: 20px;
              line-height: 1.5;
            }
            h1 {
              text-align: center;
              margin: 20px 0 40px 0;
              color: #4a9eff;
            }

            /* Log container styling */
            .logs {
              background-color: rgb(20, 20, 20);
              border: 1px solid rgb(60, 60, 60);
              border-radius: 8px;
              padding: 20px;
              margin-top: 20px;
              max-height: 70vh;
              overflow-y: auto;
              box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.3);
            }

            /* Log entry styling */
            .event {
              margin: 8px 0;
              padding: 12px;
              border-radius: 6px;
              border-left: 4px solid;
              background-color: rgba(255, 255, 255, 0.02);
              font-size: 13px;
              display: block;
              transition: background-color 0.2s ease;
              animation: slideIn 0.3s ease-out;
            }

            @keyframes slideIn {
              from {
                opacity: 0;
                transform: translateY(-10px);
              }
              to {
                opacity: 1;
                transform: translateY(0);
              }
            }

            .event:hover {
              background-color: rgba(255, 255, 255, 0.05);
            }

            /* Log level specific styling */
            .log-info {
              border-left-color: #4a9eff;
              background-color: rgba(74, 158, 255, 0.05);
            }

            .log-error {
              border-left-color: #ff6b6b;
              background-color: rgba(255, 107, 107, 0.1);
            }

            .log-warn {
              border-left-color: #feca57;
              background-color: rgba(254, 202, 87, 0.08);
            }

            .log-debug {
              border-left-color: #9c88ff;
              background-color: rgba(156, 136, 255, 0.05);
            }

            /* Log header styling */
            .log-header {
              display: flex;
              align-items: center;
              gap: 12px;
              margin-bottom: 6px;
              padding-bottom: 4px;
              border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            }

            .log-timestamp {
              color: #888;
              font-size: 11px;
              font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
              flex-shrink: 0;
            }

            .log-event-type {
              color: #4a9eff;
              font-weight: bold;
              font-size: 12px;
              text-transform: uppercase;
              flex-shrink: 0;
            }

            .log-level {
              padding: 2px 6px;
              border-radius: 3px;
              font-size: 10px;
              font-weight: bold;
              margin-left: auto;
            }

            .log-level-info {
              background-color: #4a9eff;
              color: #000;
            }

            .log-level-error {
              background-color: #ff6b6b;
              color: #fff;
            }

            .log-level-warn {
              background-color: #feca57;
              color: #000;
            }

            .log-level-debug {
              background-color: #9c88ff;
              color: #fff;
            }

            /* Log message styling */
            .log-message {
              color: #fff;
              line-height: 1.4;
              word-wrap: break-word;
            }

            /* Pre-formatted JSON styling */
            .log-meta {
              margin: 8px 0 0 0;
              padding: 8px;
              background-color: rgba(0, 0, 0, 0.3);
              border-radius: 4px;
              font-size: 11px;
              color: #ccc;
              overflow-x: auto;
              white-space: pre-wrap;
              word-wrap: break-word;
              border: 1px solid rgba(255, 255, 255, 0.1);
            }

            /* Scrollbar styling */
            .logs::-webkit-scrollbar {
              width: 8px;
            }

            .logs::-webkit-scrollbar-track {
              background: rgba(255, 255, 255, 0.1);
              border-radius: 4px;
            }

            .logs::-webkit-scrollbar-thumb {
              background: rgba(255, 255, 255, 0.3);
              border-radius: 4px;
            }

            .logs::-webkit-scrollbar-thumb:hover {
              background: rgba(255, 255, 255, 0.5);
            }

            /* Core container styling */
            .cores-container {
              margin-top: 20px;
            }

            .cores-container h2 {
              color: #4a9eff;
              margin-bottom: 20px;
              font-size: 24px;
            }

            .cores-grid {
              display: grid;
              grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
              gap: 20px;
              margin-bottom: 20px;
            }

            .core-card {
              background-color: rgba(255, 255, 255, 0.02);
              border: 1px solid rgba(255, 255, 255, 0.1);
              border-radius: 8px;
              padding: 20px;
              transition: all 0.3s ease;
            }

            .core-card:hover {
              background-color: rgba(255, 255, 255, 0.05);
              border-color: rgba(255, 255, 255, 0.2);
              transform: translateY(-2px);
              box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
            }

            .core-card.announcing {
              border-left: 4px solid #4a9eff;
            }

            .core-card.inactive {
              border-left: 4px solid #888;
              opacity: 0.7;
            }

            .core-header {
              display: flex;
              justify-content: space-between;
              align-items: center;
              margin-bottom: 15px;
              padding-bottom: 10px;
              border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            }

            .core-status {
              width: 12px;
              height: 12px;
              border-radius: 50%;
              display: inline-block;
            }

            .status-active {
              background-color: #4a9eff;
              box-shadow: 0 0 8px rgba(74, 158, 255, 0.5);
            }

            .status-inactive {
              background-color: #888;
            }

            .core-priority {
              color: #feca57;
              font-size: 12px;
              font-weight: bold;
            }

            .core-key {
              display: flex;
              align-items: center;
              gap: 8px;
              margin-bottom: 15px;
              padding: 10px;
              background-color: rgba(0, 0, 0, 0.2);
              border-radius: 6px;
            }

            .core-key label {
              color: #888;
              font-size: 12px;
              font-weight: bold;
              min-width: 30px;
            }

            .key-value {
              font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
              color: #4a9eff;
              font-size: 13px;
              flex: 1;
            }

            .copy-btn {
              background: none;
              border: 1px solid rgba(255, 255, 255, 0.2);
              border-radius: 4px;
              padding: 4px 8px;
              cursor: pointer;
              font-size: 12px;
              transition: all 0.2s ease;
            }

            .copy-btn:hover {
              background-color: rgba(255, 255, 255, 0.1);
              border-color: rgba(255, 255, 255, 0.3);
            }

            .core-stats {
              display: grid;
              grid-template-columns: repeat(3, 1fr);
              gap: 15px;
              margin-bottom: 15px;
            }

            .stat {
              text-align: center;
              padding: 8px;
              background-color: rgba(255, 255, 255, 0.02);
              border-radius: 6px;
            }

            .stat-label {
              display: block;
              color: #888;
              font-size: 11px;
              margin-bottom: 4px;
            }

            .stat-value {
              display: block;
              color: #fff;
              font-weight: bold;
              font-size: 14px;
            }

            .core-timestamps {
              display: grid;
              grid-template-columns: 1fr 1fr;
              gap: 15px;
              margin-bottom: 10px;
            }

            .timestamp {
              padding: 6px;
            }

            .timestamp-label {
              display: block;
              color: #888;
              font-size: 11px;
              margin-bottom: 2px;
            }

            .timestamp-value {
              display: block;
              color: #fff;
              font-size: 12px;
            }

            .core-referrer {
              padding: 8px;
              background-color: rgba(74, 158, 255, 0.1);
              border-radius: 6px;
              margin-top: 10px;
            }

            .referrer-label {
              color: #4a9eff;
              font-size: 11px;
              font-weight: bold;
              margin-right: 8px;
            }

            .referrer-value {
              color: #fff;
              font-size: 12px;
              font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
            }

            .no-cores {
              text-align: center;
              padding: 40px;
              color: #888;
              font-style: italic;
            }

            /* Cores section styling */
            .cores-section {
              margin: 30px 0;
            }

            .load-cores-btn {
              background: linear-gradient(135deg, #4a9eff, #357abd);
              border: none;
              color: white;
              padding: 12px 24px;
              border-radius: 6px;
              cursor: pointer;
              font-size: 14px;
              font-weight: bold;
              transition: all 0.3s ease;
              margin-bottom: 20px;
            }

            .load-cores-btn:hover {
              background: linear-gradient(135deg, #357abd, #2968a3);
              transform: translateY(-1px);
              box-shadow: 0 4px 12px rgba(74, 158, 255, 0.3);
            }

            .load-cores-btn:active {
              transform: translateY(0);
            }

            .htmx-indicator {
              display: none;
              color: #4a9eff;
              font-style: italic;
              margin-left: 10px;
            }

            .htmx-request .htmx-indicator {
              display: inline;
            }
          </style>
        </head>
        <body>
          <h1>Blind Peer</h1>

          <div hx-get="/blind-peer" hx-trigger="load">
            <span hx-indicator="true">Loading...</span>
          </div>

          <div hx-get="/cores" hx-trigger="load">
            <span hx-indicator="true">Loading...</span>
          </div>

          <div
            class="logs"
            hx-ext="sse"
            sse-connect="/sse"
            sse-swap="logs"
          ></div>
        </body>
      </html>`,
    );
  });

  serve(app);
};
