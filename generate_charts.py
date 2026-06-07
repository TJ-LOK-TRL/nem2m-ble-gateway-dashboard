"""
oneM2M BLE Sensor - Performance Charts Generator
-------------------------------------------------
Reads metrics_raw_ble.json and metrics_raw_wifi.json and generates
5 comparison charts for the project report.

Usage:
    python generate_charts.py

Output: charts/ directory with 5 PNG files
"""

import json
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

# ── Colour palette (light theme for academic paper) ───────────────────────────
BG     = '#FFFFFF'
S1     = '#F8F9FA'
S2     = '#E9ECEF'
BLE_C  = '#5B4FD9'   # purple
WIFI_C = '#1A9E4A'   # green
ACCENT = '#0077B6'   # blue
DANGER = '#D62828'
WARN   = '#E07B00'
TEXT   = '#1A1A2E'
MUTED  = '#555577'
GRID   = '#DEDEDE'

OUTPUT_DIR = 'charts'
os.makedirs(OUTPUT_DIR, exist_ok=True)

plt.rcParams.update({
    'font.family':      'DejaVu Sans',
    'font.size':        9,
    'axes.titlesize':   11,
    'axes.labelsize':   9,
    'xtick.labelsize':  8,
    'ytick.labelsize':  8,
    'figure.dpi':       150,
})

# ── Helpers ───────────────────────────────────────────────────────────────────
def style_ax(ax, title='', xlabel='', ylabel=''):
    ax.set_facecolor(S1)
    ax.tick_params(colors=MUTED)
    ax.spines[:].set_color(GRID)
    ax.xaxis.label.set_color(MUTED)
    ax.yaxis.label.set_color(MUTED)
    ax.title.set_color(TEXT)
    ax.grid(True, color=GRID, linewidth=0.6, alpha=0.8)
    if title:  ax.set_title(title, fontsize=11, pad=8, color=TEXT, fontweight='bold')
    if xlabel: ax.set_xlabel(xlabel, fontsize=9)
    if ylabel: ax.set_ylabel(ylabel, fontsize=9)

def load_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

def valid_total_lat(readings):
    return [r['total_latency_ms'] for r in readings
            if r.get('total_latency_ms') is not None and r['total_latency_ms'] > 0]

def inter_arrival(readings):
    rx = sorted([r['collector_rx_ms'] for r in readings if r.get('collector_rx_ms')])
    return [(rx[i] - rx[i-1]) for i in range(1, len(rx)) if rx[i] - rx[i-1] < 60000]

def valid_overhead(readings):
    return [r['overhead_pct'] for r in readings
            if r.get('overhead_pct') is not None and r['overhead_pct'] < 100]

def save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=BG)
    plt.close(fig)
    print(f"Saved: {path}")

# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading data...")
ble  = load_json('metrics_raw_ble.json')
wifi = load_json('metrics_raw_wifi.json')

ble_r  = ble['readings']
wifi_r = wifi['readings']
ble_s  = ble['summary']
wifi_s = wifi['summary']

ble_lat  = valid_total_lat(ble_r)
wifi_lat = valid_total_lat(wifi_r)
ble_ia   = inter_arrival(ble_r)
wifi_ia  = inter_arrival(wifi_r)
ble_oh   = valid_overhead(ble_r)
wifi_oh  = valid_overhead(wifi_r)

print(f"BLE:  {len(ble_lat)} latency samples, {len(ble_ia)} inter-arrival samples")
print(f"WiFi: {len(wifi_lat)} latency samples, {len(wifi_ia)} inter-arrival samples")

# ── Chart 1: Latency Histogram ────────────────────────────────────────────────
print("\nGenerating Chart 1: Latency Histogram...")
fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG)
fig.suptitle('Figure 1 — End-to-End Latency Distribution (Sensor → CSE → Collector)',
             color=TEXT, fontsize=12, fontweight='bold', y=1.02)

max_lat = max(max(ble_lat), max(wifi_lat))
bins = np.linspace(0, min(max_lat * 1.1, 8000), 25)

for ax, data, color, label in [
    (axes[0], ble_lat,  BLE_C,  'BLE/GATT'),
    (axes[1], wifi_lat, WIFI_C, 'WiFi/HTTP')
]:
    ax.hist(data, bins=bins, color=color, alpha=0.75, edgecolor='white', linewidth=0.5)
    ax.axvline(np.mean(data),   color=WARN,   linestyle='--', linewidth=1.8,
               label=f'Mean {np.mean(data):.0f} ms')
    ax.axvline(np.median(data), color=DANGER, linestyle=':',  linewidth=1.8,
               label=f'Median {np.median(data):.0f} ms')
    style_ax(ax, f'{label}', 'Latency (ms)', 'Message Count')
    ax.legend(fontsize=8, facecolor=BG, edgecolor=GRID)

plt.tight_layout(pad=1.5)
save(fig, '01_latency_histogram.png')

# ── Chart 2: Box Plot ─────────────────────────────────────────────────────────
print("Generating Chart 2: Latency Box Plot...")
fig, ax = plt.subplots(figsize=(8, 6), facecolor=BG)
fig.suptitle('Figure 2 — BLE vs WiFi Latency Comparison',
             color=TEXT, fontsize=12, fontweight='bold')

bp = ax.boxplot([ble_lat, wifi_lat],
    tick_labels=['BLE/GATT', 'WiFi/HTTP'],
    patch_artist=True,
    medianprops=dict(color=TEXT, linewidth=2.5),
    whiskerprops=dict(color=MUTED, linewidth=1.2),
    capprops=dict(color=MUTED, linewidth=1.2),
    flierprops=dict(marker='o', color=DANGER, markersize=4, alpha=0.5))

bp['boxes'][0].set_facecolor(BLE_C  + '55')
bp['boxes'][0].set_edgecolor(BLE_C)
bp['boxes'][1].set_facecolor(WIFI_C + '55')
bp['boxes'][1].set_edgecolor(WIFI_C)

style_ax(ax, '', 'Transport', 'Latency (ms)')
ax.tick_params(axis='x', colors=TEXT, labelsize=11)

for i, (data, color) in enumerate([(ble_lat, BLE_C), (wifi_lat, WIFI_C)], 1):
    ymax = np.percentile(data, 95)
    ax.text(i, ymax * 1.05,
            f'μ = {np.mean(data):.0f} ms\nσ = {np.std(data):.0f} ms\nn = {len(data)}',
            ha='center', va='bottom', color=color, fontsize=9, fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor=BG, edgecolor=color, alpha=0.9))

plt.tight_layout()
save(fig, '02_latency_boxplot.png')

# ── Chart 3: Inter-Arrival Timeline ──────────────────────────────────────────
print("Generating Chart 3: Inter-Arrival Timeline...")
fig, axes = plt.subplots(2, 1, figsize=(12, 7), facecolor=BG)
fig.suptitle('Figure 3 — Inter-Arrival Time Between Consecutive Messages',
             color=TEXT, fontsize=12, fontweight='bold')

for ax, ia, color, label in [
    (axes[0], ble_ia,  BLE_C,  'BLE/GATT'),
    (axes[1], wifi_ia, WIFI_C, 'WiFi/HTTP')
]:
    mean_ia = np.mean(ia)
    bar_colors = [DANGER if v > mean_ia * 3 else color for v in ia]
    ax.bar(range(len(ia)), ia, color=bar_colors, alpha=0.75, width=0.8)
    ax.axhline(mean_ia, color=WARN, linestyle='--', linewidth=1.5,
               label=f'Mean {mean_ia:.0f} ms')
    ax.axhline(1000, color=MUTED, linestyle=':', linewidth=1.2,
               label='1 s threshold')
    style_ax(ax, f'{label}', 'Message Index', 'Inter-Arrival (ms)')
    ax.legend(fontsize=8, facecolor=BG, edgecolor=GRID)
    bursts = sum(1 for v in ia if v < 1000)
    ax.text(0.98, 0.95, f'Messages < 1 s apart: {bursts} ({bursts/len(ia)*100:.0f}%)',
            transform=ax.transAxes, ha='right', va='top', fontsize=8, color=MUTED,
            bbox=dict(boxstyle='round', facecolor=BG, edgecolor=GRID, alpha=0.9))
    ax.set_ylim(0, min(np.percentile(ia, 98) * 1.3, 35000))

plt.tight_layout(pad=1.5)
save(fig, '03_interarrival_timeline.png')

# ── Chart 4: Protocol Overhead ────────────────────────────────────────────────
print("Generating Chart 4: Protocol Overhead...")
fig, axes = plt.subplots(1, 2, figsize=(12, 5), facecolor=BG)
fig.suptitle('Figure 4 — oneM2M Protocol Overhead per Message',
             color=TEXT, fontsize=12, fontweight='bold', y=1.02)

for ax, r, color, label in [
    (axes[0], [x for x in ble_r  if x.get('payload_bytes', 0) > 0], BLE_C,  'BLE/GATT'),
    (axes[1], [x for x in wifi_r if x.get('payload_bytes', 0) > 0], WIFI_C, 'WiFi/HTTP')
]:
    pay = [x['payload_bytes'] for x in r]
    cin = [x['cin_bytes']     for x in r]
    ovh = [c - p for p, c in zip(pay, cin)]
    idx = range(len(pay))
    ax.bar(idx, pay, color=ACCENT, alpha=0.8, label=f'Sensor JSON  (μ = {np.mean(pay):.0f} B)')
    ax.bar(idx, ovh, bottom=pay, color=color, alpha=0.75,
           label=f'oneM2M overhead  (μ = {np.mean(ovh):.0f} B, {np.mean(ovh)/np.mean(cin)*100:.1f}%)')
    style_ax(ax, f'{label}', 'Message Index', 'Bytes')
    ax.legend(fontsize=8, facecolor=BG, edgecolor=GRID)

plt.tight_layout(pad=1.5)
save(fig, '04_overhead_breakdown.png')

# ── Chart 5: Summary Comparison ──────────────────────────────────────────────
print("Generating Chart 5: Summary Comparison...")
fig = plt.figure(figsize=(13, 7), facecolor=BG)
fig.suptitle('Figure 5 — BLE/GATT vs WiFi/HTTP — Performance Summary',
             color=TEXT, fontsize=12, fontweight='bold')

gs = GridSpec(2, 3, figure=fig, hspace=0.6, wspace=0.45)

metrics = [
    ('Message Rate',         'msg/min',
     ble_s['collection']['message_rate_per_min'],
     wifi_s['collection']['message_rate_per_min']),
    ('Mean E2E Latency',     'ms',
     ble_s['total_latency_ms']['mean'],
     wifi_s['total_latency_ms']['mean']),
    ('Latency Std Dev',      'ms',
     ble_s['total_latency_ms']['stdev'],
     wifi_s['total_latency_ms']['stdev']),
    ('Protocol Overhead',    '%',
     round(np.mean(ble_oh), 2),
     round(np.mean(wifi_oh), 2)),
    ('Median Inter-Arrival', 'ms',
     ble_s['inter_arrival_ms']['median'],
     wifi_s['inter_arrival_ms']['median']),
    ('Messages / 5 min',     'count',
     ble_s['collection']['total_messages'],
     wifi_s['collection']['total_messages']),
]

positions = [(0,0),(0,1),(0,2),(1,0),(1,1),(1,2)]
for (row, col), (title, unit, ble_val, wifi_val) in zip(positions, metrics):
    ax = fig.add_subplot(gs[row, col])
    bars = ax.bar(['BLE', 'WiFi'], [ble_val, wifi_val],
                  color=[BLE_C, WIFI_C], alpha=0.8, width=0.5,
                  edgecolor='white', linewidth=0.5)
    style_ax(ax, title, '', unit)
    ax.tick_params(axis='x', colors=TEXT, labelsize=10)
    for bar, val in zip(bars, [ble_val, wifi_val]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() * 1.03,
                f'{val:.1f}', ha='center', va='bottom',
                color=TEXT, fontsize=9, fontweight='bold')

save(fig, '05_summary_comparison.png')

print(f"\nAll charts saved to '{OUTPUT_DIR}/'")
print("Files:", sorted(os.listdir(OUTPUT_DIR)))