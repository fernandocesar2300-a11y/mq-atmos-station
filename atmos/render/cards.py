"""Optional PNG cards + banner for social sharing. Not part of the critical path:
the web panel renders from JSON. Reads a finished status.json structure."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches


def sector_card(sec: dict, out_path: str, version: str) -> None:
    c = sec["current"]; color = c.get("color", "#2ecc71")
    fig, ax = plt.subplots(figsize=(6, 3.4), facecolor="#0f172a"); ax.set_facecolor("#0f172a")
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.add_patch(patches.Rectangle((0, 0), 0.03, 1, transform=ax.transAxes, linewidth=0, facecolor=color))
    T = ax.transAxes
    plt.text(0.08, 0.80, sec["name"], color="white", fontsize=16, fontweight="bold", transform=T)
    plt.text(0.08, 0.68, f"{sec.get('description','')} | {sec['coords']['elevation']}m", color="#94a3b8", fontsize=9, fontweight="bold", transform=T)
    plt.text(0.92, 0.68, f"{int(c['temp'])}°", color="white", fontsize=38, fontweight="bold", ha="right", transform=T)
    mri_col = "#ffffff" if c["status_rank"] >= 80 else "#38bdf8" if c["eei"] < c["temp"] else "#fca5a5"
    plt.text(0.92, 0.55, f"MRI: {c['eei']}°", color=mri_col, fontsize=10, fontweight="bold", ha="right", transform=T)
    plt.text(0.08, 0.45, f"WIND {int(c['wind'])} · GUST {int(c['gust'])} km/h", color="#e67e22" if c["gust"] >= 40 else "#94a3b8", fontsize=9, transform=T)
    lp = c.get("lightning_potential", 0)
    if lp > 10:
        plt.text(0.92, 0.45, f"⚡ {int(lp)}%", color="#8b0000" if lp > 50 else "#e74c3c" if lp > 30 else "#e67e22", fontsize=8, fontweight="bold", ha="right", transform=T)
    con = sec["consensus"]; st = con["status"]
    lbl = "⚠ SINGLE MODEL" if st == "INVALID" else (f"◈ {con['sources_active']}SRC AGREE" if st == "AGREE" else f"{'⚠' if st=='DIVERGENT' else '?'} IFS:{con['ifs_temp']}° ICON:{con['icon_temp']}° G±{con['gust_spread']:.0f}")
    plt.text(0.08, 0.35, lbl, color={"INVALID": "#e74c3c", "DIVERGENT": "#e67e22", "UNCERTAIN": "#f1c40f"}.get(st, "#475569"), fontsize=9, fontweight="bold", transform=T)
    plt.text(0.92, 0.25, f" {c['status']} ", color="white", fontsize=9, fontweight="bold", ha="right",
             bbox=dict(boxstyle="round,pad=0.4", fc=color, ec="none", alpha=0.9), transform=T)
    plt.plot([0.05, 0.95], [0.15, 0.15], color="#334155", linewidth=1, transform=T)
    for x, ha, key in ((0.05, "left", "forecast_3h"), (0.95, "right", "forecast_6h")):
        f = sec[key]; arrow = "(-)" if f["eei"] - c["eei"] < -2 else "(+)" if f["eei"] - c["eei"] > 2 else "(=)"
        plt.text(x, 0.09, f"{key[-2:].upper()}: {int(f['temp'])}° G{int(f['gust'])} {arrow}", color="#94a3b8", fontsize=9, fontweight="bold", ha=ha, transform=T)
    ax.axis("off"); plt.savefig(out_path, dpi=150, facecolor="#0f172a"); plt.close()


def banner(status: dict, out_path: str, track_points, version: str) -> None:
    s = status["summary"]; rk = s["alert_rank"]
    color = "#8b0000" if rk >= 90 else "#e74c3c" if rk >= 70 else "#e67e22" if rk >= 60 else "#f1c40f" if rk >= 40 else "#2ecc71"
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor="#0a0a0a"); ax.set_facecolor("#0a0a0a")
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0); T = ax.transAxes
    ax.add_patch(patches.Rectangle((0, 0), 0.015, 1, transform=T, linewidth=0, facecolor=color))
    if track_points:
        ax_r = fig.add_axes([0.05, 0.15, 0.20, 0.70]); ax_r.set_facecolor("#0a0a0a")
        ax_r.plot([p[1] for p in track_points], [p[0] for p in track_points], color=color, linewidth=1.2, alpha=0.9)
        ax_r.set_aspect("equal"); ax_r.axis("off")
    plt.text(0.28, 0.70, f"{status['event_name'].upper()} · DSS FIELD", color="white", fontsize=14, fontweight="bold", transform=T)
    hook = "ALL SECTORS: GO" if rk == 0 else f"{s['alert_level']} — {s['worst_sector']}"
    plt.text(0.28, 0.50, hook, color=color, fontsize=10, fontweight="bold", transform=T)
    src = "⚠ SINGLE MODEL" if status["consensus_engine"]["models_identical_detected"] else "IFS · ICON-EU · IPMA"
    plt.text(0.28, 0.35, f"UPDATED {status['last_update']} UTC | {src} | ATMOS {version}", color="#888", fontsize=8, transform=T)
    plt.plot([0.68, 0.68], [0.2, 0.8], color="#222", linewidth=1, transform=T)
    plt.text(0.76, 0.70, "MIN MRI", color="#666", fontsize=7, ha="center", transform=T)
    plt.text(0.76, 0.45, f"{s['min_mri']}°", color="#38bdf8" if (s["min_mri"] or 99) < 10 else "white", fontsize=20, fontweight="bold", ha="center", transform=T)
    plt.text(0.90, 0.70, "MAX GUST", color="#666", fontsize=7, ha="center", transform=T)
    plt.text(0.90, 0.45, f"{int(s['max_gust'] or 0)}", color="#e67e22" if (s["max_gust"] or 0) > 40 else "white", fontsize=20, fontweight="bold", ha="center", transform=T)
    plt.text(0.90, 0.32, "km/h", color="#666", fontsize=7, ha="center", transform=T)
    ax.axis("off"); plt.savefig(out_path, facecolor="#0a0a0a", dpi=150); plt.close()
