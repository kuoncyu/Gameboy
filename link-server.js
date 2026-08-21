#!/usr/bin/env node
/**
 * link-server.js
 * ------------------------------------------------------------
 * 給 Game Boy 模擬器「連線對戰」用的小型區域網路中繼伺服器。
 *
 * 瀏覽器本身無法被動接受連線（沒辦法像伺服器一樣「監聽」某個 IP
 * 讓別的裝置直接連進來），所以需要這支小程式在同一個 WiFi 網路
 * 內的某台裝置上執行，充當兩支手機之間的中繼站：
 *
 *   - 一台手機在遊戲設定裡選「Server（伺服器）」→ 瀏覽器會連到
 *     這支程式（通常是同一台裝置上的 http://localhost:8787）。
 *   - 另一台手機選「Client（客戶端）」，輸入這台裝置的區網 IP
 *     （例如 192.168.1.23），連到 ws://192.168.1.23:8787。
 *   - 兩邊都連上後，這支程式會把它們配成一對，之後遊戲的連線
 *     對戰資料就會透過這支程式即時互相轉送。
 *
 * 同時，這支程式也會把同一個資料夾底下的 index.html（遊戲本體）
 * 用 HTTP 靜態檔案的方式提供出來，所以最簡單的用法是：
 *
 *   1. 把 index.html 和 link-server.js 放在同一個資料夾。
 *   2. 安裝相依套件：  npm install ws
 *   3. 啟動伺服器：    node link-server.js
 *      （預設監聽 8787 埠，可用 PORT 環境變數或第一個參數更改）
 *   4. 找出這台裝置的區網 IP（例如在 Windows 用 ipconfig、
 *      macOS/Linux 用 ifconfig 或 ip addr，手機 Termux 用 ip addr）。
 *   5. Server 那台手機瀏覽器開 http://<這台裝置的IP>:8787/ ，
 *      在設定裡選「Server」。
 *      Client 那台手機開同一份 index.html（可以是同一個網址，
 *      也可以是另外下載的檔案），選「Client」並輸入上面那個 IP。
 *
 * 資料完全只在同一個區域網路內傳輸，這支程式不會把任何資料送到
 * 網際網路上。
 * ------------------------------------------------------------
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");

let WebSocketServer;
try {
  WebSocketServer = require("ws").WebSocketServer;
} catch (err) {
  console.error(
    "找不到 'ws' 套件，請先在這個資料夾執行：npm install ws\n"
  );
  process.exit(1);
}

const PORT = parseInt(process.argv[2] || process.env.PORT || "8787", 10);
const PUBLIC_DIR = __dirname;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".gb": "application/octet-stream",
  ".gbc": "application/octet-stream",
};

// ---- 順便提供靜態檔案（主要是 index.html），方便直接用瀏覽器打開 ----
const httpServer = http.createServer((req, res) => {
  let reqPath = decodeURIComponent(req.url.split("?")[0]);
  if (reqPath === "/") reqPath = "/index.html";
  const filePath = path.join(PUBLIC_DIR, reqPath);

  // 防止路徑跳出資料夾
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("找不到檔案：" + reqPath);
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(data);
  });
});

// ---- WebSocket 中繼／配對邏輯 ----
const wss = new WebSocketServer({ server: httpServer });

let hostSocket = null;  // 目前的 Server（伺服器）連線
let guestSocket = null; // 目前的 Client（客戶端）連線

function sendJSON(sock, obj) {
  if (sock && sock.readyState === sock.OPEN) {
    try { sock.send(JSON.stringify(obj)); } catch (err) {}
  }
}

function tryPair() {
  if (hostSocket && guestSocket) {
    sendJSON(hostSocket, { t: "paired" });
    sendJSON(guestSocket, { t: "paired" });
    console.log("[link-server] 已配對成功，開始轉送資料");
  }
}

wss.on("connection", (ws) => {
  ws._role = null;

  ws.on("message", (data, isBinary) => {
    // 第一則訊息通常是文字 JSON：{ t:"hello", role:"host"|"guest" }
    if (!isBinary && ws._role === null) {
      let msg;
      try { msg = JSON.parse(data.toString()); } catch (err) { msg = null; }
      if (msg && msg.t === "hello" && (msg.role === "host" || msg.role === "guest")) {
        if (msg.role === "host") {
          if (hostSocket && hostSocket !== ws) {
            try { hostSocket.close(); } catch (err) {}
          }
          hostSocket = ws;
          ws._role = "host";
          console.log("[link-server] Server 端已連線");
        } else {
          if (guestSocket && guestSocket !== ws) {
            try { guestSocket.close(); } catch (err) {}
          }
          guestSocket = ws;
          ws._role = "guest";
          console.log("[link-server] Client 端已連線");
        }
        tryPair();
        return;
      }
    }

    // 之後的訊息（不論文字或二進位）都原封不動轉給另一邊
    const peer = ws._role === "host" ? guestSocket : ws._role === "guest" ? hostSocket : null;
    if (peer && peer.readyState === peer.OPEN) {
      try { peer.send(data, { binary: isBinary }); } catch (err) {}
    }
  });

  ws.on("close", () => {
    if (ws._role === "host" && hostSocket === ws) {
      hostSocket = null;
      console.log("[link-server] Server 端已離線");
      sendJSON(guestSocket, { t: "peer-left" });
    } else if (ws._role === "guest" && guestSocket === ws) {
      guestSocket = null;
      console.log("[link-server] Client 端已離線");
      sendJSON(hostSocket, { t: "peer-left" });
    }
  });

  ws.on("error", () => {});
});

httpServer.listen(PORT, () => {
  console.log("========================================");
  console.log(" 連線對戰中繼伺服器已啟動");
  console.log(" 監聽埠：" + PORT);
  console.log("");
  console.log(" 這台裝置在區域網路內的 IP 位址可能是：");
  const nets = os.networkInterfaces();
  Object.keys(nets).forEach((name) => {
    (nets[name] || []).forEach((net) => {
      if (net.family === "IPv4" && !net.internal) {
        console.log("   http://" + net.address + ":" + PORT + "/");
      }
    });
  });
  console.log("");
  console.log(" 這台裝置：於遊戲設定選「Server（伺服器）」");
  console.log(" 另一台裝置：於遊戲設定選「Client（客戶端）」，");
  console.log("            並輸入上面列出的其中一組 IP 位址");
  console.log("========================================");
});
