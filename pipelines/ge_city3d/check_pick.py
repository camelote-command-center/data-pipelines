#!/usr/bin/env python3
"""Headless check of a built commune: tileset loads without errors, and a click straight down on a known tree
returns that tree's height_m. Usage: check_pick.py <tileset rel path> <cache cell key>"""
import json, subprocess, sys, tempfile, time, urllib.request, shutil
import numpy as np, websocket
from pyproj import Transformer
TS, CELL = sys.argv[1], sys.argv[2]
d = np.load(f"canton_trees/{CELL}.npz"); i = int(np.argmax(d["h"]))
x, y, h, z = (float(d[k][i]) for k in "xyhz")
lon, lat = Transformer.from_crs(2056, 4326, always_xy=True).transform(x, y)
CH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"; PORT = 9334
prof = tempfile.mkdtemp()
p = subprocess.Popen([CH, "--headless=new", f"--remote-debugging-port={PORT}", f"--user-data-dir={prof}", "--use-angle=metal", "--enable-gpu",
                      "--ignore-gpu-blocklist", "--window-size=1200,800", f"--remote-allow-origins=http://127.0.0.1:{PORT}",
                      f"http://127.0.0.1:8765/cesium_view_v4.html?ts={TS}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(60):
        try: tabs = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/json", timeout=2)); break
        except Exception: time.sleep(0.5)
    ws = websocket.create_connection(next(t for t in tabs if t["type"] == "page")["webSocketDebuggerUrl"], timeout=60)
    n = [0]
    def ev(js):
        n[0] += 1; ws.send(json.dumps({"id": n[0], "method": "Runtime.evaluate", "params": {"expression": js, "awaitPromise": True, "returnByValue": True}}))
        while True:
            m = json.loads(ws.recv())
            if m.get("id") == n[0]: return m["result"]["result"].get("value")
    for _ in range(60):
        if ev("!!window.ts"): break
        time.sleep(1)
    js = f"""(async () => {{
      const v = window.viewer, s = v.scene; v.camera.cancelFlight();
      v.camera.setView({{destination: Cesium.Cartesian3.fromDegrees({lon}, {lat}, {z + h + 80}),
                         orientation: {{heading: 0, pitch: -Cesium.Math.PI_OVER_TWO, roll: 0}}}});
      const t0 = Date.now(); while (Date.now() - t0 < 60000) {{ await new Promise(r => setTimeout(r, 500)); if (window.ts.tilesLoaded) break; }}
      await new Promise(r => setTimeout(r, 1500));
      const c = new Cesium.Cartesian2(s.canvas.clientWidth / 2, s.canvas.clientHeight / 2);
      const f = s.pick(c);
      return {{loaded: window.ts.tilesLoaded, height_m: f && f.getProperty ? f.getProperty('height_m') : null,
               crown_m: f && f.getProperty ? f.getProperty('crown_m') : null, egid: f && f.getProperty ? f.getProperty('egid') : null,
               errors: (window.__errors || []).length,
               drawn: (() => {{ const out = []; const walk = t => {{ if (t.content && t.content.url && t._selectedFrame === s.frameState.frameNumber) out.push(t.content.url.split('/').slice(-2).join('/').split('?')[0]); (t.children||[]).forEach(walk); }}; walk(window.ts.root); return out.slice(0, 12); }})(),
               cam_h: v.camera.positionCartographic.height,
               drill: s.drillPick(c, 5).map(p => p && p.getProperty ? ['feat', p.getProperty('height_m'), p.getProperty('egid')] : [p && p.primitive ? p.primitive.constructor.name : String(p)]),
               pos_h: (() => {{ const q = s.pickPosition(c); return q ? Cesium.Cartographic.fromCartesian(q).height : null; }})(),
               probes: [[350,570],[430,580],[490,620],[300,200],[1000,420],[600,356]].map(([x,y]) => {{ const p = s.pick(new Cesium.Cartesian2(x*s.canvas.clientWidth/1200, y*s.canvas.clientHeight/713)); return [x, y, p && p.getProperty ? [p.getProperty('kind'), p.getProperty('egid'), p.getProperty('height_m')] : (p ? 'nonfeature' : null)]; }}),
               canvas: [s.canvas.clientWidth, s.canvas.clientHeight, s.canvas.width, s.canvas.height, window.devicePixelRatio],
               shot: (s.render(), s.canvas.toDataURL('image/jpeg', 0.7))}};
    }})()"""
    r = ev(js)
    import base64; shot = r.pop("shot", None)
    if shot: open(f"/tmp/pick_{CELL}.jpg", "wb").write(base64.b64decode(shot.split(",")[1]))
    print(json.dumps({"tileset": TS, "tree": {"x": x, "y": y, "detected_h": round(h, 2)}, "picked": r}))
finally:
    p.terminate(); p.wait(timeout=10); shutil.rmtree(prof, ignore_errors=True)
