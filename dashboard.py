"""
oneM2M Dashboard - Consumer AE
-------------------------------
Independent AE that subscribes to the CSE and displays sensor data.
This component has NO knowledge of the gateway or how data arrives at the CSE.
It only knows the CSE - pure oneM2M consumer pattern.

Architecture:
    [ACME CSE] --m2m:sub notify--> [dashboard.py] --WebSocket--> [browser]
    [ACME CSE] --HTTP GET--------> [dashboard.py] (history)
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Set

import requests
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Configuration ─────────────────────────────────────────────────────────────
CSE_BASE         = "http://127.0.0.1:8080"
CSE_ID           = "cse-in"
AE_NAME          = "android-sensor"
CONTAINER_NAME   = "sensorData"
ORIGINATOR       = "CDashboard"
NOTIFICATION_URL = "http://127.0.0.1:9000/notify"
SUB_NAME         = "dashboard-sub"

DASHBOARD_PORT   = 9000

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dashboard")

# ── Shared state ──────────────────────────────────────────────────────────────
connected_ws: Set[WebSocket] = set()
latest_data: dict = {}
history: list = []
MAX_HISTORY = 60
last_reception_time = 0.0

dashboard_status = {
    "cse": "idle",
    "sub": "none",
    "receptions": 0,
    "last_reception": None,
    "transport": "-"
}

# ── oneM2M helpers ────────────────────────────────────────────────────────────
def cse_headers(ty: int = None, ri: str = None) -> dict:
    h = {
        "X-M2M-Origin": ORIGINATOR,
        "X-M2M-RI": ri or f"req-{int(time.time()*1000)}",
        "X-M2M-RVI": "3",
        "Accept": "application/json",
    }
    if ty is not None:
        h["Content-Type"] = f"application/json;ty={ty}"
    return h

def ensure_dashboard_ae():
    """Register dashboard as a separate AE in the CSE."""
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": "dashboard-ae", "api": "N.onem2m.dashboard", "rr": True, "srv": ["3"]}})
    log.info(f"Dashboard AE: {r.status_code}")

def ensure_subscription():
    """Create subscription on the sensor container - CSE will push to /notify."""
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{CONTAINER_NAME}"
    # Delete old sub
    requests.delete(f"{url}/{SUB_NAME}",
        headers=cse_headers(ri="del-sub"), timeout=5)
    # Create new sub
    r = requests.post(url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": SUB_NAME,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3]}
        }})
    ok = r.status_code in (200, 201)
    log.info(f"Subscription: {r.status_code} {r.text[:80]}")
    dashboard_status["sub"] = "active" if ok else f"failed {r.status_code}"

def fetch_history_from_cse(limit: int = 20) -> list:
    """Retrieve last N ContentInstances directly from CSE."""
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{CONTAINER_NAME}"
    try:
        r = requests.get(url, timeout=5,
            params={"rcn": 4, "ty": 4, "lim": limit},
            headers=cse_headers(ri="hist-fetch"))
        if r.status_code == 200:
            data = r.json()
            cins = data.get("m2m:cnt", {}).get("m2m:cin", [])
            if isinstance(cins, dict):
                cins = [cins]
            result = []
            for cin in cins:
                try:
                    con = cin.get("con", "{}")
                    parsed = json.loads(con) if isinstance(con, str) else con
                    result.append({"ts": cin.get("ct", ""), "data": parsed})
                except Exception:
                    pass
            return result
    except Exception as e:
        log.warning(f"History fetch failed: {e}")
    return []

# ── WebSocket broadcast ───────────────────────────────────────────────────────
async def broadcast(message: dict):
    dead = set()
    for ws in connected_ws:
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    connected_ws.difference_update(dead)

# ── Sensor data handler ───────────────────────────────────────────────────────
async def watchdog():
    """Reset status if no data received for 15 seconds."""
    global last_reception_time
    while True:
        await asyncio.sleep(5)
        if last_reception_time > 0 and time.time() - last_reception_time > 30:
            dashboard_status["cse"] = "idle"
            dashboard_status["transport"] = "-"
            await broadcast({"type": "status", "data": dashboard_status})

async def handle_notification(con: str, transport: str):
    global latest_data, last_reception_time
    last_reception_time = time.time()
    try:
        parsed = json.loads(con)
        parsed["_received_at"] = time.time()
        parsed["_transport"] = transport
        latest_data = parsed

        history.append({"ts": time.time(), "data": parsed})
        if len(history) > MAX_HISTORY:
            history.pop(0)

        dashboard_status["cse"] = "receiving"
        dashboard_status["receptions"] += 1
        dashboard_status["last_reception"] = time.strftime("%H:%M:%S")
        dashboard_status["transport"] = transport

        await broadcast({"type": "sensors", "data": parsed})
        await broadcast({"type": "status", "data": dashboard_status})
        await broadcast({"type": "history", "data": history[-20:]})
        await broadcast({"type": "log", "msg": f"Data via {transport} · batt={parsed.get('battery_pct','-')}%", "level": "ok"})

    except Exception as e:
        log.warning(f"Notification parse error: {e}")

# ── FastAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ensure_dashboard_ae()
    except Exception as e:
        log.warning(f"AE registration failed: {e}")
    asyncio.create_task(delayed_subscription())
    asyncio.create_task(watchdog())
    yield

async def delayed_subscription():
    await asyncio.sleep(3)
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, ensure_subscription)
        await broadcast({"type": "status", "data": dashboard_status})
    except Exception as e:
        log.warning(f"Subscription failed: {e}")

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.get("/api/history")
async def get_history():
    cse_hist = fetch_history_from_cse(20)
    return {"local": history[-20:], "cse": cse_hist}

@app.get("/api/status")
async def get_status():
    return dashboard_status

# oneM2M Subscription notification endpoint - CSE calls this
@app.post("/notify")
async def receive_notification(request: Request):
    try:
        body = await request.json()
        log.info("CSE push notification received")

        sgn = body.get("m2m:sgn", {})
        nev = sgn.get("nev", {})
        rep = nev.get("rep", {})
        cin = rep.get("m2m:cin", {})
        con = cin.get("con", "")

        # Extract transport from labels
        lbl = cin.get("lbl", [])
        transport = "unknown"
        for label in lbl:
            if label.startswith("transport:"):
                transport = label.split(":", 1)[1]
                break

        if con:
            await handle_notification(con, transport)

    except Exception as e:
        log.warning(f"Notification error: {e}")

    return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_ws.add(websocket)
    await websocket.send_json({"type": "status", "data": dashboard_status})
    if latest_data:
        await websocket.send_json({"type": "sensors", "data": latest_data})
    if history:
        await websocket.send_json({"type": "history", "data": history[-20:]})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_ws.discard(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard:app", host="0.0.0.0", port=DASHBOARD_PORT, reload=False)