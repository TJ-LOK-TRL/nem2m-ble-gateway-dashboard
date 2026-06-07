"""
oneM2M BLE Gateway - Multi-Sensor Support
------------------------------------------
Interworking Proxy AE - translates BLE sensor data to oneM2M ContentInstances.
Supports multiple simultaneous BLE sensors, each with its own Container.
The gateway has NO knowledge of the dashboard - it only talks to the CSE.

Uses aiohttp for async HTTP POST to CSE, avoiding blocking the event loop
when multiple sensors send data simultaneously (burst handling).

Architecture:
    [Sensor BLE 1] --BLE--> |                  |
    [Sensor BLE 2] --BLE--> | gateway.py (AE)  | --Mca/HTTP--> [ACME CSE]
    [Sensor BLE N] --BLE--> |                  |
    [Android WiFi] --------------------------------Mca/HTTP--> [ACME CSE]
"""

import asyncio
import logging
import re
import time

import aiohttp
import requests
from bleak import BleakClient, BleakScanner

# -- Configuration -------------------------------------------------------------
BLE_DEVICE_NAMES = ["OneM2M-Sensor"]
BLE_CHAR_UUID    = "12345678-1234-1234-1234-123456789abd"

CSE_BASE         = "http://192.168.1.232:8080"
CSE_ID           = "cse-in"
AE_NAME          = "sensor-ae"
ORIGINATOR       = "CAndroid"

POLL_INTERVAL    = 5

# -- Logging -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gateway")

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

def device_to_container_name(device_name: str) -> str:
    """Stable container name based on device name (not MAC, which changes)."""
    return "sensor-" + re.sub(r'[^a-zA-Z0-9]', '', device_name).lower()

def ensure_ae_and_acp():
    """One-time setup at startup: create ACP and AE (synchronous, called once)."""
    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(1),
        json={"m2m:acp": {
            "rn": "acp-sensor",
            "pv": {"acr": [{"acor": ["CDashboard", "CAndroid", "CAdmin", "CMetrics"], "acop": 63}]},
            "pvs": {"acr": [{"acor": ["CAdmin"], "acop": 63}]}
        }})
    log.info(f"ACP: {r.status_code}")

    r = requests.post(f"{CSE_BASE}/{CSE_ID}", timeout=5,
        headers=cse_headers(2),
        json={"m2m:ae": {
            "rn": AE_NAME,
            "api": "N.onem2m.sensor",
            "rr": True,
            "srv": ["3"],
            "acpi": ["/id-in/cse-in/acp-sensor"]
        }})
    log.info(f"AE: {r.status_code}")

def ensure_container(container_name: str, device_name: str, address: str) -> bool:
    """Create sensor container if it doesn't exist (synchronous, called once per device)."""
    r = requests.post(f"{CSE_BASE}/{CSE_ID}/{AE_NAME}", timeout=5,
        headers=cse_headers(3),
        json={"m2m:cnt": {
            "rn": container_name,
            "mni": 200,
            "acpi": ["/id-in/cse-in/acp-sensor"],
            "lbl": [f"device:{device_name}", f"address:{address}", "type:ble-sensor"]
        }})
    log.info(f"Container [{container_name}]: {r.status_code}")
    return r.status_code in (200, 201, 409)

async def post_cin(container_name: str, sensor_json: str, device_name: str) -> int:
    """
    Async POST of a ContentInstance to the CSE.
    Using aiohttp allows multiple sensors to POST concurrently without
    blocking each other — important for burst handling in multi-sensor setups.
    """
    url = f"{CSE_BASE}/{CSE_ID}/{AE_NAME}/{container_name}"
    body = {"m2m:cin": {
        "con": sensor_json,
        "cnf": "application/json:0",
        "lbl": [
            "transport:BLE/GATT",
            f"device:{device_name}",
            "source:gateway"
        ]
    }}
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=cse_headers(4), json=body, timeout=timeout) as r:
            return r.status

# -- BLE sensor handler --------------------------------------------------------
async def handle_sensor(device_name: str, address: str):
    container_name = device_to_container_name(device_name)
    ensure_container(container_name, device_name, address)

    while True:
        log.info(f"[{device_name}] Connecting to {address}...")
        try:
            async with BleakClient(address) as client:
                log.info(f"[{device_name}] Connected")

                def notification_handler(sender, data: bytearray):
                    asyncio.get_event_loop().call_soon_threadsafe(
                        asyncio.ensure_future,
                        process_data(data, container_name, device_name)
                    )

                await client.start_notify(BLE_CHAR_UUID, notification_handler)
                log.info(f"[{device_name}] Subscribed to BLE notifications")

                while client.is_connected:
                    try:
                        await client.read_gatt_char(BLE_CHAR_UUID)
                    except Exception:
                        break
                    await asyncio.sleep(POLL_INTERVAL)

                log.info(f"[{device_name}] Disconnected")

        except Exception as e:
            log.warning(f"[{device_name}] BLE error: {e}")
            await asyncio.sleep(5)

async def process_data(data: bytearray, container_name: str, device_name: str):
    try:
        sensor_json = data.decode("utf-8")
        code = await post_cin(container_name, sensor_json, device_name)
        log.info(f"[{device_name}] CSE POST {code}")
    except Exception as e:
        log.warning(f"[{device_name}] Error: {e}")

# -- Scanner -------------------------------------------------------------------
async def scanner_loop():
    active_devices = {}

    while True:
        log.info(f"Scanning for {BLE_DEVICE_NAMES}...")
        try:
            devices = await BleakScanner.discover(timeout=10.0)
            for device in devices:
                if device.name in BLE_DEVICE_NAMES and device.address not in active_devices:
                    log.info(f"Found: {device.name} ({device.address})")
                    task = asyncio.create_task(handle_sensor(device.name, device.address))
                    active_devices[device.address] = task

            finished = [addr for addr, t in active_devices.items() if t.done()]
            for addr in finished:
                del active_devices[addr]

        except Exception as e:
            log.warning(f"Scanner error: {e}")

        await asyncio.sleep(15)

# -- Main ----------------------------------------------------------------------
async def main():
    try:
        ensure_ae_and_acp()
    except Exception as e:
        log.warning(f"CSE setup failed: {e}")
    await scanner_loop()

if __name__ == "__main__":
    asyncio.run(main())