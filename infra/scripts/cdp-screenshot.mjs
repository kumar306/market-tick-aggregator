// Screenshots a web page with headless Edge/Chrome over the DevTools protocol, waiting REAL seconds for the page
// to finish drawing. The plain `--screenshot` flag cannot do this: it captures at load, before a Grafana
// dashboard has fetched and drawn its panels.
//
// node cdp-screenshot.mjs <browser.exe> <url> <out.png> [waitSeconds=25] [width=1920] [height=1320]
// Needs Node 22+ (global WebSocket). No npm packages.
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const [browser, url, out, waitArg = "25", wArg = "1920", hArg = "1320"] = process.argv.slice(2);
if (!browser || !url || !out) {
  console.error("usage: cdp-screenshot.mjs <browser> <url> <out.png> [waitSeconds] [width] [height]");
  process.exit(2);
}
const waitMs = Number(waitArg) * 1000, width = Number(wArg), height = Number(hArg);
const port = 9333 + Math.floor(Math.random() * 500);
const profile = mkdtempSync(join(tmpdir(), "cdp-"));

const proc = spawn(browser, [
  "--headless=new", "--disable-gpu", "--hide-scrollbars", `--remote-debugging-port=${port}`,
  `--user-data-dir=${profile}`, `--window-size=${width},${height}`, "--no-first-run", "about:blank",
], { stdio: "ignore" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const cleanup = () => { try { proc.kill(); } catch {} try { rmSync(profile, { recursive: true, force: true }); } catch {} };

async function pageTarget() {
  for (let i = 0; i < 60; i++) {
    try {
      const list = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
      const page = list.find((t) => t.type === "page");
      if (page) return page.webSocketDebuggerUrl;
    } catch {}
    await sleep(500);
  }
  throw new Error("browser debugging endpoint never came up");
}

try {
  const ws = new WebSocket(await pageTarget());
  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  let id = 0;
  const pending = new Map();
  ws.onmessage = (m) => { const d = JSON.parse(m.data); if (d.id && pending.has(d.id)) { pending.get(d.id)(d); pending.delete(d.id); } };
  const send = (method, params = {}) => new Promise((res) => { const i = ++id; pending.set(i, res); ws.send(JSON.stringify({ id: i, method, params })); });

  await send("Page.enable");
  await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
  await send("Page.navigate", { url });
  await sleep(waitMs);
  const shot = await send("Page.captureScreenshot", { format: "png" });
  if (!shot.result?.data) throw new Error("no screenshot data: " + JSON.stringify(shot.error));
  writeFileSync(out, Buffer.from(shot.result.data, "base64"));
  console.log(`wrote ${out}`);
  ws.close();
} catch (e) {
  console.error("screenshot failed:", e.message);
  process.exitCode = 1;
} finally {
  cleanup();
}
