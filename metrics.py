"""
oneM2M Metrics Collector - Independent Consumer AE
---------------------------------------------------
Third independent AE that subscribes to sensor containers and collects
performance metrics. Demonstrates CSE serving multiple consumer AEs simultaneously.

Latency measurement:
  - sensor_ts_ms: timestamp_ms from Android sensor (moment of data creation)
  - cse_ct_ms:    ct (creation time) from oneM2M CIN (moment data arrived at CSE)
  - collector_rx_ts: when metrics collector received the notification

  cse_latency   = cse_ct_ms - sensor_ts_ms      (Android -> CSE, main metric)
  notify_latency = collector_rx_ts*1000 - cse_ct_ms  (CSE -> collector)
  total_latency  = collector_rx_ts*1000 - sensor_ts_ms

Using cse_ct_ms avoids clock skew issues between Android and PC for
notification latency, while cse_latency uses both clocks (noted as limitation).
"""

import asyncio
import json
import logging
import statistics
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import List, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse

# -- Configuration -------------------------------------------------------------
CSE_BASE            = "http://127.0.0.1:8080"
CSE_ID              = "cse-in"
AE_NAME             = "sensor-ae"
ORIGINATOR          = "CMetrics"
NOTIFICATION_URL    = "http://127.0.0.1:9001/notify"
METRICS_PORT        = 9001
COLLECTION_DURATION = 300  # seconds

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metrics")
log.setLevel(logging.DEBUG)

# -- Data structures -----------------------------------------------------------
def parse_acme_timestamp(ct: str) -> Optional[float]:
    """
    Parse ACME CSE creation time format: '20260606T231933,024400'
    Returns Unix timestamp in milliseconds as float, or None if unparseable.
    ACME stores ct in local system time. Microseconds are included for precision.
    """
    try:
        # Format: YYYYMMDDTHHmmss,ffffff
        parts = ct.split(",")
        from datetime import datetime
        dt = datetime.strptime(parts[0], "%Y%m%dT%H%M%S")
        base_ms = dt.timestamp() * 1000
        # Add microseconds if present (format: 6 digits = microseconds)
        if len(parts) > 1:
            micros = int(parts[1])
            base_ms += micros / 1000  # convert microseconds to milliseconds
        return base_ms  # return ms directly
    except Exception:
        return None

@dataclass
class SensorReading:
    sensor_ts_ms: int          # Android timestamp_ms (ms since epoch)
    cse_ct_ms: Optional[float] # CSE creation time in ms (from CIN ct field)
    collector_rx_ts: float     # when collector received notification (s)
    transport: str
    device: str
    payload_bytes: int
    cin_bytes: int

@dataclass
class MetricsState:
    readings: List[SensorReading] = field(default_factory=list)
    readings_archive: List[SensorReading] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    subscribed_containers: set = field(default_factory=set)
    collection_active: bool = False
    has_live_data: bool = False

state = MetricsState()

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

def register_ae():
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": "metrics-ae", "api": "N.onem2m.metrics",
                         "rr": True, "srv": ["3"]}})
    log.info(f"Metrics AE: {r.status_code}")

def subscribe_ae_discovery():
    ae_url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}"
    requests.delete(f"{ae_url}/metrics-discovery-sub",
        headers=cse_headers(ri="del"), timeout=5)
    r = requests.post(ae_url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": "metrics-discovery-sub",
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3], "chty": [3]}
        }})
    log.info(f"AE discovery sub: {r.status_code}")

def subscribe_container(container_name: str) -> bool:
    if container_name in state.subscribed_containers:
        return True
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    sub_name = f"metrics-sub-{container_name}"
    requests.delete(f"{url}/{sub_name}", headers=cse_headers(ri="del"), timeout=5)
    r = requests.post(url, timeout=15,
        headers=cse_headers(23),
        json={"m2m:sub": {
            "rn": sub_name,
            "nu": [NOTIFICATION_URL],
            "nct": 1,
            "enc": {"net": [3]}
        }})
    ok = r.status_code in (200, 201)
    if ok:
        state.subscribed_containers.add(container_name)
        log.info(f"Subscribed to [{container_name}]")
    return ok

def fetch_existing_containers() -> list:
    try:
        r = requests.get(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}",
            params={"rcn": 6, "ty": 3},
            headers=cse_headers(ri="list"), timeout=5)
        if r.status_code == 200:
            refs = r.json().get("m2m:rrl", {}).get("rrf", [])
            return [ref["nm"] for ref in refs if ref.get("typ") == 3]
    except Exception as e:
        log.warning(f"Container list failed: {e}")
    return []

def fetch_latest_from_container(container_name: str) -> list:
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    try:
        r = requests.get(url, timeout=5,
            params={"rcn": 4, "ty": 4, "lim": 3},
            headers=cse_headers(ri="fetch-seed"))
        if r.status_code == 200:
            data = r.json()
            cins = data.get("m2m:cnt", {}).get("m2m:cin", [])
            if isinstance(cins, dict):
                cins = [cins]
            return cins
    except Exception as e:
        log.warning(f"Fetch seed failed: {e}")
    return []

# -- Metrics calculation -------------------------------------------------------
def calculate_metrics() -> dict:
    readings = state.readings
    if not readings:
        return {"error": "No data collected"}

    duration = state.end_time - state.start_time if state.end_time > 0 else time.time() - state.start_time

    # CSE latency: Android -> CSE (uses ct from CIN, avoids notification delay)
    cse_latencies = []
    for r in readings:
        if r.sensor_ts_ms > 0 and r.cse_ct_ms is not None:
            lat = r.cse_ct_ms - r.sensor_ts_ms
            log.debug(f"cse_lat={lat:.1f}ms transport={r.transport}")
            if -5000 < lat < 120000:  # allow small negative for clock skew
                cse_latencies.append(lat)

    # Notification latency: CSE -> collector
    notify_latencies = []
    for r in readings:
        if r.cse_ct_ms is not None:
            lat = (r.collector_rx_ts * 1000) - r.cse_ct_ms
            log.debug(f"notify_lat={lat:.1f}ms")
            if 0 < lat < 120000:
                notify_latencies.append(lat)

    # Total latency: Android -> CSE -> collector
    total_latencies = []
    for r in readings:
        if r.sensor_ts_ms > 0:
            lat = (r.collector_rx_ts * 1000) - r.sensor_ts_ms
            if -5000 < lat < 120000:
                total_latencies.append(lat)

    # Inter-arrival times
    sorted_r = sorted(readings, key=lambda r: r.collector_rx_ts)
    inter_arrivals = []
    for i in range(1, len(sorted_r)):
        dt = (sorted_r[i].collector_rx_ts - sorted_r[i-1].collector_rx_ts) * 1000
        inter_arrivals.append(dt)

    # Burst detection
    bursts = sum(1 for ia in inter_arrivals if ia < 1000)

    # Payload analysis
    payload_sizes = [r.payload_bytes for r in readings]
    cin_sizes = [r.cin_bytes for r in readings]
    overhead = [(c - p) / c * 100 for p, c in zip(payload_sizes, cin_sizes) if c > 0]

    # Transport breakdown
    ble_count  = sum(1 for r in readings if 'BLE' in r.transport)
    wifi_count = sum(1 for r in readings if 'WiFi' in r.transport or 'HTTP' in r.transport)

    devices = {}
    for r in readings:
        devices[r.device] = devices.get(r.device, 0) + 1

    def safe_stats(data):
        if not data:
            return {"min": 0, "max": 0, "mean": 0, "median": 0, "stdev": 0, "n": 0}
        return {
            "n":      len(data),
            "min":    round(min(data), 2),
            "max":    round(max(data), 2),
            "mean":   round(statistics.mean(data), 2),
            "median": round(statistics.median(data), 2),
            "stdev":  round(statistics.stdev(data), 2) if len(data) > 1 else 0
        }

    return {
        "collection": {
            "duration_s":           round(duration, 1),
            "total_messages":       len(readings),
            "message_rate_per_min": round(len(readings) / duration * 60, 2) if duration > 0 else 0,
            "containers_observed":  list(state.subscribed_containers),
            "devices":              devices
        },
        "cse_latency_ms":     safe_stats(cse_latencies),
        "notify_latency_ms":  safe_stats(notify_latencies),
        "total_latency_ms":   safe_stats(total_latencies),
        "inter_arrival_ms":   safe_stats(inter_arrivals),
        "burst_analysis": {
            "burst_messages":    bursts,
            "burst_percentage":  round(bursts / len(inter_arrivals) * 100, 1) if inter_arrivals else 0,
            "burst_threshold_ms": 1000
        },
        "payload_bytes":        safe_stats(payload_sizes),
        "cin_bytes":            safe_stats(cin_sizes),
        "oneM2M_overhead_pct":  safe_stats(overhead),
        "transport_breakdown": {
            "BLE_GATT":  ble_count,
            "WiFi_HTTP": wifi_count,
            "unknown":   len(readings) - ble_count - wifi_count
        }
    }

def format_report(metrics: dict) -> str:
    if "error" in metrics:
        return f"ERROR: {metrics['error']}"

    c    = metrics["collection"]
    cse  = metrics["cse_latency_ms"]
    ntf  = metrics["notify_latency_ms"]
    tot  = metrics["total_latency_ms"]
    ia   = metrics["inter_arrival_ms"]
    b    = metrics["burst_analysis"]
    pay  = metrics["payload_bytes"]
    cin  = metrics["cin_bytes"]
    oh   = metrics["oneM2M_overhead_pct"]
    tr   = metrics["transport_breakdown"]

    return f"""
========================================================
  oneM2M BLE SENSOR - PERFORMANCE METRICS REPORT
========================================================

COLLECTION SUMMARY
  Duration:          {c['duration_s']} s
  Total messages:    {c['total_messages']}
  Message rate:      {c['message_rate_per_min']} msg/min
  Devices:           {c['devices']}
  Containers:        {c['containers_observed']}

LATENCY BREAKDOWN
  [1] Android -> CSE  (sensor_ts to CIN creation time)
      Samples: {cse['n']}
      Min:     {cse['min']} ms
      Max:     {cse['max']} ms
      Mean:    {cse['mean']} ms
      Median:  {cse['median']} ms
      Stdev:   {cse['stdev']} ms

  [2] CSE -> Collector  (CIN creation time to notification receipt)
      Samples: {ntf['n']}
      Min:     {ntf['min']} ms
      Max:     {ntf['max']} ms
      Mean:    {ntf['mean']} ms
      Median:  {ntf['median']} ms
      Stdev:   {ntf['stdev']} ms

  [3] Total End-to-End  (Android -> CSE -> Collector)
      Samples: {tot['n']}
      Min:     {tot['min']} ms
      Max:     {tot['max']} ms
      Mean:    {tot['mean']} ms
      Median:  {tot['median']} ms
      Stdev:   {tot['stdev']} ms

INTER-ARRIVAL TIME  (between consecutive messages at collector)
  Min:     {ia['min']} ms
  Max:     {ia['max']} ms
  Mean:    {ia['mean']} ms
  Median:  {ia['median']} ms
  Stdev:   {ia['stdev']} ms

BLE BATCHING ANALYSIS
  Burst messages:  {b['burst_messages']} ({b['burst_percentage']}%)
  Threshold:       < {b['burst_threshold_ms']} ms between messages
  Note: High burst % confirms Android BLE/WiFi scheduler buffering

PAYLOAD ANALYSIS
  Raw sensor JSON:   min={pay['min']}B  mean={pay['mean']}B  max={pay['max']}B
  oneM2M CIN total:  min={cin['min']}B  mean={cin['mean']}B  max={cin['max']}B
  Protocol overhead: mean={oh['mean']}%  max={oh['max']}%

TRANSPORT BREAKDOWN
  BLE/GATT:    {tr['BLE_GATT']} messages
  WiFi/HTTP:   {tr['WiFi_HTTP']} messages
  Unknown:     {tr['unknown']} messages

========================================================
"""

# -- Notification handler ------------------------------------------------------
async def process_cin(cin: dict, live: bool = True):
    rx_ts = time.time()

    con = cin.get("con", "")
    lbl = cin.get("lbl", [])
    ct  = cin.get("ct", "")   # CSE creation time

    transport = "unknown"
    device    = "unknown"
    for label in lbl:
        if label.startswith("transport:"):
            transport = label.split(":", 1)[1]
        elif label.startswith("device:"):
            device = label.split(":", 1)[1]

    # Parse CSE creation time
    cse_ct_s = parse_acme_timestamp(ct)
    cse_ct_ms = cse_ct_s  # parse_acme_timestamp now returns ms directly

    sensor_ts_ms  = 0
    payload_bytes = 0
    try:
        parsed = json.loads(con) if isinstance(con, str) else con
        sensor_ts_ms  = parsed.get("timestamp_ms", 0)
        payload_bytes = len(con.encode()) if isinstance(con, str) else len(json.dumps(con).encode())
    except Exception:
        pass

    cin_bytes = len(json.dumps(cin).encode())

    if live and not state.has_live_data:
        state.has_live_data = True
        log.info("Live data confirmed - collection will start shortly")

    if not state.collection_active:
        return

    reading = SensorReading(
        sensor_ts_ms    = sensor_ts_ms,
        cse_ct_ms       = cse_ct_ms,
        collector_rx_ts = rx_ts,
        transport       = transport,
        device          = device,
        payload_bytes   = payload_bytes,
        cin_bytes       = cin_bytes
    )
    state.readings.append(reading)
    state.readings_archive.append(reading)

    count = len(state.readings)
    if count % 10 == 0:
        elapsed   = rx_ts - state.start_time
        remaining = max(0, COLLECTION_DURATION - elapsed)
        log.info(f"Collected {count} messages | {remaining:.0f}s remaining")

# -- Collection timer ----------------------------------------------------------
async def collection_timer():
    await asyncio.sleep(8)
    log.info("Waiting for live sensor data...")
    while not state.has_live_data:
        await asyncio.sleep(1)
    await asyncio.sleep(2)
    state.readings.clear()
    state.readings_archive.clear()
    log.info(f"=== COLLECTION STARTED ({COLLECTION_DURATION}s) ===")
    state.start_time    = time.time()
    state.collection_active = True
    await asyncio.sleep(COLLECTION_DURATION)
    state.end_time          = time.time()
    state.collection_active = False
    log.info("=== COLLECTION COMPLETE ===")
    metrics = calculate_metrics()
    report  = format_report(metrics)
    print(report)
    with open("metrics_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
        f.write("\n\nRAW JSON:\n")
        f.write(json.dumps(metrics, indent=2))
    log.info("Report saved to metrics_report.txt")

    # Save raw readings as separate JSON for chart generation
    transport_label = "wifi" if any("WiFi" in r.transport or "HTTP" in r.transport for r in state.readings_archive) else "ble"
    raw_file = f"metrics_raw_{transport_label}.json"
    raw_data = {
        "transport": transport_label,
        "summary": calculate_metrics(),
        "readings": [
            {
                "sensor_ts_ms": r.sensor_ts_ms,
                "cse_ct_ms": r.cse_ct_ms,
                "collector_rx_ms": r.collector_rx_ts * 1000,
                "transport": r.transport,
                "device": r.device,
                "payload_bytes": r.payload_bytes,
                "cin_bytes": r.cin_bytes,
                "cse_latency_ms": round(r.cse_ct_ms - r.sensor_ts_ms, 2) if r.cse_ct_ms and r.sensor_ts_ms else None,
                "notify_latency_ms": round(r.collector_rx_ts * 1000 - r.cse_ct_ms, 2) if r.cse_ct_ms else None,
                "total_latency_ms": round(r.collector_rx_ts * 1000 - r.sensor_ts_ms, 2) if r.sensor_ts_ms else None,
                "overhead_pct": round((r.cin_bytes - r.payload_bytes) / r.cin_bytes * 100, 2) if r.cin_bytes else None
            }
            for r in state.readings_archive
        ]
    }
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)
    log.info(f"Raw data saved to {raw_file}")

# -- FastAPI -------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        register_ae()
    except Exception as e:
        log.warning(f"AE failed: {e}")
    asyncio.create_task(delayed_setup())
    asyncio.create_task(collection_timer())
    yield

async def delayed_setup():
    await asyncio.sleep(3)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, subscribe_ae_discovery)
    containers = await loop.run_in_executor(None, fetch_existing_containers)
    log.info(f"Existing containers: {containers}")
    for container in containers:
        ok = await loop.run_in_executor(None, subscribe_container, container)
        if ok:
            cins = await loop.run_in_executor(None, fetch_latest_from_container, container)
            if cins:
                log.info(f"Seed data found in [{container}]")
                #state.has_live_data = True

app = FastAPI(lifespan=lifespan)

@app.post("/notify")
async def receive_notification(request: Request):
    try:
        body = await request.json()
        sgn  = body.get("m2m:sgn", {})
        if sgn.get("vrq"):
            return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)
        nev = sgn.get("nev", {})
        rep = nev.get("rep", {})
        cnt = rep.get("m2m:cnt")
        if cnt:
            rn = cnt.get("rn", "")
            if rn:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, subscribe_container, rn)
        cin = rep.get("m2m:cin")
        if cin:
            await process_cin(cin, live=True)
    except Exception as e:
        log.warning(f"Notification error: {e}")
    return JSONResponse(content={"m2m:rsp": {"rsc": 2000}}, status_code=200)

@app.get("/report")
async def get_report():
    if state.collection_active:
        elapsed   = time.time() - state.start_time
        remaining = max(0, COLLECTION_DURATION - elapsed)
        return {"status": "collecting", "messages": len(state.readings),
                "elapsed_s": round(elapsed, 1), "remaining_s": round(remaining, 1)}
    if not state.readings:
        return {"status": "waiting", "has_live_data": state.has_live_data}
    return calculate_metrics()

@app.get("/report/text")
async def get_report_text():
    metrics = calculate_metrics()
    report  = format_report(metrics)
    return HTMLResponse(f"<pre style='font-family:monospace;padding:20px'>{report}</pre>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("metrics:app", host="0.0.0.0", port=METRICS_PORT, reload=False)