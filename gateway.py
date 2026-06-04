"""
oneM2M BLE Gateway
------------------
Interworking Proxy AE - translates BLE sensor data to oneM2M ContentInstances.
This component has NO knowledge of the dashboard or any consumer AE.
It only knows the CSE.

Architecture:
    [Android BLE] --BLE--> [gateway.py] --Mca/HTTP--> [ACME CSE]
    [Android WiFi] ----------------------Mca/HTTP--> [ACME CSE]
"""

import asyncio
import json
import logging
import time

import requests
from bleak import BleakClient, BleakScanner

# ── Configuration ─────────────────────────────────────────────────────────────
BLE_DEVICE_NAME = "OneM2M-Sensor"
BLE_CHAR_UUID   = "12345678-1234-1234-1234-123456789abd"

CSE_BASE        = "http://127.0.0.1:8080"
CSE_ID          = "cse-in"
AE_NAME         = "android-sensor"
CONTAINER_NAME  = "sensorData"
ORIGINATOR      = "CAndroid"

POLL_INTERVAL   = 5

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gateway")

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

def ensure_acp():
    """Create Access Control Policy allowing CDashboard to subscribe."""
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(1),
        json={"m2m:acp": {
            "rn": "acp-sensor",
            "pv": {
                "acr": [{
                    "acor": ["CDashboard", "CAndroid", "CAdmin"],
                    "acop": 63  # all permissions
                }]
            },
            "pvs": {
                "acr": [{
                    "acor": ["CAdmin"],
                    "acop": 63
                }]
            }
        }})
    log.info(f"ACP: {r.status_code} {r.text[:80]}")
    return r.status_code in (200, 201, 409)

def ensure_ae_and_container():
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {"rn": AE_NAME, "api": "N.onem2m.sensor", "rr": True, "srv": ["3"]}})
    log.info(f"AE: {r.status_code}")

    ensure_acp()

    r = requests.post(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}", timeout=5,
        headers=cse_headers(3),
        json={"m2m:cnt": {"rn": CONTAINER_NAME, "mni": 200,
                  "acpi": ["/id-in/cse-in/acp-sensor"]}})
    log.info(f"Container: {r.status_code}")

def post_cin(sensor_json: str, transport: str = "BLE/GATT") -> int:
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{CONTAINER_NAME}"
    r = requests.post(url, timeout=5,
        headers=cse_headers(4),
        json={"m2m:cin": {
            "con": sensor_json,
            "cnf": "application/json:0",
            "lbl": [f"transport:{transport}", "device:android-sensor"]
        }})
    return r.status_code

# ── Sensor data handler ───────────────────────────────────────────────────────
async def handle_sensor_data(data: bytearray):
    try:
        sensor_json = data.decode("utf-8")
        code = post_cin(sensor_json)
        log.info(f"CSE POST {code}")
    except Exception as e:
        log.warning(f"Error: {e}")

# ── BLE gateway loop ──────────────────────────────────────────────────────────
async def ble_gateway_loop():
    while True:
        log.info(f"Scanning for '{BLE_DEVICE_NAME}'...")
        device = None
        try:
            device = await BleakScanner.find_device_by_name(BLE_DEVICE_NAME, timeout=15.0)
        except Exception as e:
            log.warning(f"Scan error: {e}")

        if device is None:
            log.warning("Device not found, retrying in 5s...")
            await asyncio.sleep(5)
            continue

        log.info(f"Found: {device.address}")
        try:
            async with BleakClient(device) as client:
                log.info("BLE connected")

                def notification_handler(sender, data: bytearray):
                    asyncio.get_event_loop().call_soon_threadsafe(
                        asyncio.ensure_future, handle_sensor_data(data)
                    )

                await client.start_notify(BLE_CHAR_UUID, notification_handler)
                log.info("Subscribed to BLE notifications")

                while client.is_connected:
                    try:
                        await client.read_gatt_char(BLE_CHAR_UUID)
                    except Exception:
                        break
                    await asyncio.sleep(POLL_INTERVAL)

                log.info("BLE disconnected")

        except Exception as e:
            log.warning(f"BLE error: {e}")
            await asyncio.sleep(3)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    try:
        ensure_ae_and_container()
    except Exception as e:
        log.warning(f"CSE setup failed: {e}")
    await ble_gateway_loop()

if __name__ == "__main__":
    asyncio.run(main())