"""
oneM2M BLE Gateway
------------------
1. Scans for the Android BLE GATT server (OneM2M-Sensor)
2. Reads sensor data via GATT notifications
3. Posts to ACME CSE as m2m:cin ContentInstance
4. Creates oneM2M Subscription for push notifications from CSE
5. Broadcasts to connected WebSocket clients (dashboard)
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Set

import requests
from bleak import BleakClient, BleakScanner
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Configuration ─────────────────────────────────────────────────────────────
BLE_DEVICE_NAME  = "OneM2M-Sensor"
BLE_CHAR_UUID    = "12345678-1234-1234-1234-123456789abd"

CSE_BASE         = "http://127.0.0.1:8080"
CSE_ID           = "cse-in"
AE_NAME          = "android-sensor"
CONTAINER_NAME   = "sensorData"
ORIGINATOR       = "CAndroid"
NOTIFICATION_URL = "http://127.0.0.1:9000/notify"
SUB_NAME         = "dashboard-sub"

POLL_INTERVAL    = 5

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gateway")

# ── Shared state ──────────────────────────────────────────────────────────────
connected_ws: Set[WebSocket] = set()
latest_data: dict = {}
history: list = []
MAX_HISTORY = 60

gateway_status = {
    "ble": "scanning",
    "cse": "idle",
    "posts": 0,
    "last_post": None,
    "sub": "none"
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

def ensure_ae_and_container():
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": AE_NAME, "api": "N.onem2m.sensor", "rr": True, "srv": ["3"]}})
    log.info(f"AE: {r.status_code} {r.text[:100]}")
    r = requests.post(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}", timeout=5,
        headers=cse_headers(3),
        json={"m2m:cnt": {"rn": CONTAINER_NAME, "mni": 200}})
    log.info(f"Container: {r.status_code} {r.text[:100]}")

def ensure_subscription():
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{CONTAINER_NAME}"
    requests.delete(f"{url}/{SUB_NAME}",
        headers=cse_headers(ri="del-sub"), timeout=5)
    r = requests.post(url, timeout=10,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": SUB_NAME,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3]}
        }})
    ok = r.status_code in (200, 201)
    log.info(f"Subscription: {r.status_code} {r.text[:100]}")
    gateway_status["sub"] = "active" if ok else f"failed {r.status_code}"

def post_cin(sensor_json: str) -> int:
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{CONTAINER_NAME}"
    r = requests.post(url, timeout=5,
        headers=cse_headers(4),
        json={"m2m:cin": {"con": sensor_json, "cnf": "application/json:0"}})
    return r.status_code

def fetch_history_from_cse(limit: int = 20) -> list:
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
async def handle_sensor_data(raw, source: str = "ble"):
    global latest_data
    try:
        sensor_json = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        parsed = json.loads(sensor_json)
        parsed["_received_at"] = time.time()
        parsed["_source"] = source
        latest_data = parsed

        history.append({"ts": time.time(), "data": parsed})
        if len(history) > MAX_HISTORY:
            history.pop(0)

        # Only POST to CSE if data came from BLE, not from CSE itself
        if source != "cse-push":
            try:
                code = post_cin(sensor_json if isinstance(sensor_json, str) else json.dumps(parsed))
                gateway_status["cse"] = f"OK {code}"
                gateway_status["posts"] += 1
                gateway_status["last_post"] = time.strftime("%H:%M:%S")
            except Exception as e:
                gateway_status["cse"] = f"error: {str(e)[:30]}"

        await broadcast({"type": "sensors", "data": parsed})
        await broadcast({"type": "status", "data": gateway_status})
        await broadcast({"type": "history", "data": history[-20:]})

    except Exception as e:
        log.warning(f"Data parse error: {e}")

# ── BLE gateway loop ──────────────────────────────────────────────────────────
async def ble_gateway_loop():
    while True:
        gateway_status["ble"] = "scanning"
        await broadcast({"type": "status", "data": gateway_status})
        log.info(f"Scanning for '{BLE_DEVICE_NAME}'...")

        device = None
        try:
            device = await BleakScanner.find_device_by_name(BLE_DEVICE_NAME, timeout=15.0)
        except Exception as e:
            log.warning(f"Scan error: {e}")

        if device is None:
            gateway_status["ble"] = "not found"
            await broadcast({"type": "status", "data": gateway_status})
            await asyncio.sleep(5)
            continue

        log.info(f"Found: {device.address}")
        gateway_status["ble"] = f"connected ({device.address[-5:]})"

        try:
            async with BleakClient(device) as client:
                log.info("BLE connected")
                await broadcast({"type": "status", "data": gateway_status})

                def notification_handler(sender, data: bytearray):
                    asyncio.get_event_loop().call_soon_threadsafe(
                        asyncio.ensure_future, handle_sensor_data(data, "ble")
                    )

                await client.start_notify(BLE_CHAR_UUID, notification_handler)
                log.info("Subscribed to BLE notifications")

                while client.is_connected:
                    try:
                        await client.read_gatt_char(BLE_CHAR_UUID)
                    except Exception:
                        break
                    await asyncio.sleep(POLL_INTERVAL)

                gateway_status["ble"] = "disconnected"
                gateway_status["cse"] = "idle"
                await broadcast({"type": "status", "data": gateway_status})

        except Exception as e:
            log.warning(f"BLE error: {e}")
            gateway_status["ble"] = f"error: {str(e)[:30]}"
            await broadcast({"type": "status", "data": gateway_status})
            await asyncio.sleep(3)

# ── FastAPI ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ensure_ae_and_container()
    except Exception as e:
        log.warning(f"CSE setup failed: {e}")
    task = asyncio.create_task(ble_gateway_loop())
    asyncio.create_task(setup_subscription())
    yield
    task.cancel()

async def setup_subscription():
    await asyncio.sleep(3)
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, ensure_subscription)
        await broadcast({"type": "status", "data": gateway_status})
    except Exception as e:
        log.warning(f"Subscription setup failed: {e}")
        
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.get("/api/latest")
async def get_latest():
    return {"sensors": latest_data, "status": gateway_status}

@app.get("/api/history")
async def get_history():
    cse_hist = fetch_history_from_cse(20)
    return {"local": history[-20:], "cse": cse_hist}

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
        if con:
            await handle_sensor_data(
                con.encode() if isinstance(con, str) else json.dumps(con).encode(),
                "cse-push"
            )
            await broadcast({"type": "log", "msg": "CSE push notification received", "level": "ble"})
    except Exception as e:
        log.warning(f"Notification parse error: {e}")
    return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_ws.add(websocket)
    await websocket.send_json({"type": "status", "data": gateway_status})
    if latest_data:
        await websocket.send_json({"type": "sensors", "data": latest_data})
    if history:
        await websocket.send_json({"type": "history", "data": history[-20:]})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_ws.discard(websocket)

@app.get("/setup-sub")
async def setup_sub():
    ensure_subscription()
    return {"status": gateway_status["sub"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("gateway:app", host="0.0.0.0", port=9000, reload=False)