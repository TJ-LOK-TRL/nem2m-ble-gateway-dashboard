"""
oneM2M Dashboard - Consumer AE (Multi-Sensor, Event-Driven)
------------------------------------------------------------
Independent Consumer AE implementing the full oneM2M subscription pattern:

1. Subscribes to the AE with chty=3 (Container creation notifications)
2. When gateway creates a new sensor Container, CSE notifies dashboard
3. Dashboard automatically creates a subscription on the new Container
4. CSE notifies dashboard when new ContentInstances arrive in any Container
5. On container discovery, fetches any data already present

This is the correct oneM2M pattern for dynamic multi-sensor discovery.
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

# -- Configuration -------------------------------------------------------------
CSE_BASE          = "http://127.0.0.1:8080"
CSE_ID            = "cse-in"
AE_NAME           = "sensor-ae"
ORIGINATOR        = "CDashboard"
NOTIFICATION_URL  = "http://127.0.0.1:9000/notify"
SUB_AE_NAME       = "ae-discovery-sub"
SUB_DATA_PREFIX   = "data-sub"
DASHBOARD_PORT    = 9000
WATCHDOG_TIMEOUT  = 30

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dashboard")

# -- Shared state --------------------------------------------------------------
connected_ws: Set[WebSocket] = set()
sensors: dict = {}
subscribed_containers: set = set()
last_reception_time = 0.0

dashboard_status = {
    "sub_ae": "none",
    "sub_containers": 0,
    "receptions": 0,
    "last_reception": None,
    "active_sensors": 0
}

# -- oneM2M helpers ------------------------------------------------------------
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
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": "dashboard-ae", "api": "N.onem2m.dashboard",
                         "rr": True, "srv": ["3"]}})
    code = r.status_code
    log.info(f"Dashboard AE: {'ready' if code in (201, 409, 403) else code}")

def subscribe_ae_for_container_discovery():
    ae_url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}"
    requests.delete(f"{ae_url}/{SUB_AE_NAME}",
        headers=cse_headers(ri="del-ae-sub"), timeout=5)
    r = requests.post(ae_url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": SUB_AE_NAME,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3], "chty": [3]}
        }})
    ok = r.status_code in (200, 201)
    log.info(f"AE discovery subscription: {r.status_code}")
    dashboard_status["sub_ae"] = "active" if ok else f"failed {r.status_code}"
    return ok

def subscribe_container_for_data(container_name: str) -> bool:
    if container_name in subscribed_containers:
        return True
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    sub_name = f"{SUB_DATA_PREFIX}-{container_name}"
    requests.delete(f"{url}/{sub_name}",
        headers=cse_headers(ri="del-data-sub"), timeout=5)
    r = requests.post(url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": sub_name,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3]}
        }})
    ok = r.status_code in (200, 201)
    log.info(f"Container subscription [{container_name}]: {r.status_code}")
    if ok:
        subscribed_containers.add(container_name)
        dashboard_status["sub_containers"] = len(subscribed_containers)
    return ok

def fetch_existing_containers() -> list:
    try:
        r = requests.get(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}",
            params={"rcn": 6, "ty": 3},
            headers=cse_headers(ri="list-cnt"), timeout=5)
        if r.status_code == 200:
            refs = r.json().get("m2m:rrl", {}).get("rrf", [])
            return [ref["nm"] for ref in refs if ref.get("typ") == 3]
    except Exception as e:
        log.warning(f"Container list failed: {e}")
    return []

def fetch_latest_from_container(container_name: str) -> list:
    """Fetch most recent ContentInstances already in a container."""
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    try:
        r = requests.get(url, timeout=5,
            params={"rcn": 4, "ty": 4, "lim": 5},
            headers=cse_headers(ri="fetch-latest"))
        if r.status_code == 200:
            data = r.json()
            cins = data.get("m2m:cnt", {}).get("m2m:cin", [])
            if isinstance(cins, dict):
                cins = [cins]
            return cins
    except Exception as e:
        log.warning(f"Fetch latest failed: {e}")
    return []

# -- WebSocket broadcast -------------------------------------------------------
async def broadcast(message: dict):
    dead = set()
    for ws in connected_ws:
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    connected_ws.difference_update(dead)

# -- Notification handler ------------------------------------------------------
async def handle_cin_received(cin: dict):
    global last_reception_time
    last_reception_time = time.time()

    con = cin.get("con", "")
    lbl = cin.get("lbl", [])

    transport = "unknown"
    device_name = "unknown"
    for label in lbl:
        if label.startswith("transport:"):
            transport = label.split(":", 1)[1]
        elif label.startswith("device:"):
            device_name = label.split(":", 1)[1]

    try:
        parsed = json.loads(con) if isinstance(con, str) else con
    except Exception:
        return

    parsed["_transport"] = transport
    parsed["_device"] = device_name
    parsed["_received_at"] = time.time()

    if device_name not in sensors:
        sensors[device_name] = {"latest": {}, "history": []}

    sensors[device_name]["latest"] = parsed
    sensors[device_name]["history"].append({"ts": time.time(), "data": parsed})
    if len(sensors[device_name]["history"]) > 60:
        sensors[device_name]["history"].pop(0)

    dashboard_status["receptions"] += 1
    dashboard_status["last_reception"] = time.strftime("%H:%M:%S")
    dashboard_status["active_sensors"] = len(sensors)

    await broadcast({"type": "sensors", "data": parsed, "device": device_name})
    await broadcast({"type": "status", "data": dashboard_status})
    await broadcast({"type": "history", "device": device_name,
                     "data": sensors[device_name]["history"][-20:]})
    await broadcast({"type": "log",
                     "msg": f"[{device_name}] via {transport} - batt={parsed.get('battery_pct','?')}%",
                     "level": "ok"})

async def handle_container_created(rn: str):
    log.info(f"New container discovered: {rn}")
    loop = asyncio.get_event_loop()

    # Subscribe to the new container
    ok = await loop.run_in_executor(None, subscribe_container_for_data, rn)
    if ok:
        await broadcast({"type": "status", "data": dashboard_status})
        await broadcast({"type": "log",
                         "msg": f"New sensor discovered: {rn} - subscribed", "level": "ble"})

        # Fetch any data already in the container
        cins = await loop.run_in_executor(None, fetch_latest_from_container, rn)
        for cin in cins:
            await handle_cin_received(cin)

# -- Watchdog ------------------------------------------------------------------
async def watchdog():
    global last_reception_time
    while True:
        await asyncio.sleep(5)
        if last_reception_time > 0 and time.time() - last_reception_time > WATCHDOG_TIMEOUT:
            last_reception_time = 0.0
            # Remove sensors that haven't sent data recently
            now = time.time()
            stale = [device for device, sdata in sensors.items()
                     if sdata["history"] and now - sdata["history"][-1]["ts"] > WATCHDOG_TIMEOUT]
            for device in stale:
                del sensors[device]
            dashboard_status["active_sensors"] = len(sensors)
            await broadcast({"type": "status", "data": dashboard_status})

# -- FastAPI -------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        ensure_dashboard_ae()
    except Exception as e:
        log.warning(f"AE failed: {e}")
    asyncio.create_task(delayed_setup())
    asyncio.create_task(watchdog())
    yield

async def delayed_setup():
    await asyncio.sleep(3)
    loop = asyncio.get_event_loop()

    # Step 1: AE-level subscription for future container discovery
    await loop.run_in_executor(None, subscribe_ae_for_container_discovery)
    await broadcast({"type": "status", "data": dashboard_status})

    # Step 2: Subscribe to existing containers and fetch their latest data
    containers = await loop.run_in_executor(None, fetch_existing_containers)
    log.info(f"Existing containers: {containers}")
    for container in containers:
        ok = await loop.run_in_executor(None, subscribe_container_for_data, container)
        if ok:
            cins = await loop.run_in_executor(None, fetch_latest_from_container, container)
            for cin in cins:
                await handle_cin_received(cin)
    await broadcast({"type": "status", "data": dashboard_status})

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.get("/api/sensors")
async def get_sensors():
    return {"sensors": {k: v["latest"] for k, v in sensors.items()},
            "status": dashboard_status}

@app.post("/notify")
async def receive_notification(request: Request):
    try:
        body = await request.json()
        sgn = body.get("m2m:sgn", {})

        if sgn.get("vrq"):
            log.info("CSE verification - acknowledged")
            return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)

        nev = sgn.get("nev", {})
        rep = nev.get("rep", {})

        # Container created in AE
        cnt = rep.get("m2m:cnt")
        if cnt:
            rn = cnt.get("rn", "")
            if rn:
                asyncio.create_task(handle_container_created(rn))

        # ContentInstance created in Container
        cin = rep.get("m2m:cin")
        if cin:
            asyncio.create_task(handle_cin_received(cin))

    except Exception as e:
        log.warning(f"Notification error: {e}")

    return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_ws.add(websocket)
    await websocket.send_json({"type": "status", "data": dashboard_status})
    for device, sdata in sensors.items():
        if sdata["latest"]:
            await websocket.send_json({"type": "sensors", "data": sdata["latest"], "device": device})
        if sdata["history"]:
            await websocket.send_json({"type": "history", "device": device,
                                       "data": sdata["history"][-20:]})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_ws.discard(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard:app", host="0.0.0.0", port=DASHBOARD_PORT, reload=False)