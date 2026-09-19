#!/usr/bin/env python3
"""Run prof.html scenarios in a fresh headless Chrome each (real GPU via ANGLE/Metal, empty cache)."""
import json, subprocess, sys, tempfile, time, urllib.request, shutil
import websocket

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
SCENARIOS = sys.argv[1:] or ["", "buildings", "buildings,trees", "buildings,trees,parcels", "buildings,pc", "city"]
PORT = 9333


def gpu_info(ws):
    ws.send(json.dumps({"id": 99, "method": "SystemInfo.getInfo"}))
    while True:
        m = json.loads(ws.recv())
        if m.get("id") == 99:
            g = m.get("result", {}).get("gpu", {}).get("devices", [{}])
            return (g[0].get("deviceString") or g[0].get("vendorString") or "?") if g else "?"


results = []
for sc in SCENARIOS:
    prof_dir = tempfile.mkdtemp(prefix="prof_")
    p = subprocess.Popen([CHROME, "--headless=new", f"--remote-debugging-port={PORT}", f"--user-data-dir={prof_dir}",
                          "--use-angle=metal", "--enable-gpu", "--ignore-gpu-blocklist", "--window-size=1400,900",
                          "--disable-background-timer-throttling", "--disable-renderer-backgrounding", "--no-first-run", "--remote-allow-origins=http://127.0.0.1:9333",
                          f"http://127.0.0.1:8765/prof.html?layers={sc}"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                tabs = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json", timeout=2)); break
            except Exception:
                time.sleep(0.5)
        page = next(t for t in tabs if t.get("type") == "page")
        ws = websocket.create_connection(page["webSocketDebuggerUrl"], timeout=30)
        browser = websocket.create_connection(json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json/version"))["webSocketDebuggerUrl"])
        gpu = gpu_info(browser)
        res, t0 = None, time.time()
        while time.time() - t0 < 180:
            ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {"expression": "JSON.stringify(window.__prof||null)"}}))
            while True:
                m = json.loads(ws.recv())
                if m.get("id") == 1:
                    break
            v = m["result"]["result"].get("value")
            if v and v != "null":
                res = json.loads(v); break
            time.sleep(2)
        res = res or {"layers": sc or "terrain_only", "error": "timeout"}
        res["gpu"] = gpu
        results.append(res)
        print(json.dumps(res), flush=True)
    finally:
        p.terminate(); p.wait(timeout=10); shutil.rmtree(prof_dir, ignore_errors=True)
json.dump(results, open("prof_results.json", "w"), indent=1)
