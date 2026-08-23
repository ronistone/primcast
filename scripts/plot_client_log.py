#!/usr/bin/env python3
"""Plot latency/throughput graphs from primcast client logs (client.out.NN).

Log format (TSV, one header line starting with '#'):

    # ORDER  LATENCY  SEND_AT  DEST
    1        2671     13111    (0,1,2)

LATENCY and SEND_AT are microseconds; SEND_AT is relative to client start.
DEST is the destination GidSet: a single gid = local message, more = global.

The file is parsed as a stream (never fully materialised as text), so it
handles multi-hundred-MB logs.

Usage:
    .venv-plot/bin/python scripts/plot_client_log.py client.out.00 [more.out ...]
        [--bucket 1.0] [--warmup 5] [--out plots] [--max-latency-ms 50]
"""

import argparse
import os
import sys
from array import array
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

US = 1_000_000.0
PERCENTILES = (50, 95, 99, 99.9)


def parse(path, warmup_s):
    """Stream a client log.

    Returns (buckets, latencies, first_ts_us, last_ts_us) where buckets maps
    kind -> {bucket_index: array('i', latencies_us)} and latencies maps
    kind -> array('i') of every latency, kind in {'local', 'global'}.
    Bucketing is done by the caller-supplied bucket size via `bucket_s`.
    """
    buckets = {"local": defaultdict(lambda: array("i")),
               "global": defaultdict(lambda: array("i"))}
    lat_all = {"local": array("i"), "global": array("i")}
    first = None
    last = 0
    skipped = 0
    bad = 0

    with open(path, "r", buffering=1 << 20) as fh:
        for line in fh:
            if not line or line[0] == "#":
                continue
            parts = line.split("\t")
            if len(parts) < 4:
                bad += 1
                continue
            try:
                latency = int(parts[1])
                send_at = int(parts[2])
            except ValueError:
                bad += 1
                continue
            dest = parts[3].strip()
            if first is None:
                first = send_at
            if send_at - first < warmup_s * US:
                skipped += 1
                continue
            last = send_at
            kind = "global" if "," in dest else "local"
            idx = int((send_at - first) / (parse.bucket_s * US))
            buckets[kind][idx].append(latency)
            lat_all[kind].append(latency)

    if first is None:
        sys.exit(f"{path}: no data rows")
    return buckets, lat_all, first, last, skipped, bad


def pct_series(bucket_map, bucket_s):
    """bucket_index -> latencies  =>  (times_s, {pct: values_ms}, rate_per_s)."""
    idxs = sorted(bucket_map)
    times = np.array(idxs, dtype=float) * bucket_s
    out = {p: np.empty(len(idxs)) for p in PERCENTILES}
    mean = np.empty(len(idxs))
    rate = np.empty(len(idxs))
    for i, idx in enumerate(idxs):
        vals = np.frombuffer(bucket_map[idx], dtype=np.int32)
        qs = np.percentile(vals, PERCENTILES)
        for p, q in zip(PERCENTILES, qs):
            out[p][i] = q / 1000.0
        mean[i] = vals.mean() / 1000.0
        rate[i] = len(vals) / bucket_s
    return times, out, mean, rate


def summarise(name, lat):
    a = np.frombuffer(lat, dtype=np.int32)
    if a.size == 0:
        return f"  {name:<7} none"
    qs = np.percentile(a, PERCENTILES)
    return (f"  {name:<7} n={a.size:>10,}  mean={a.mean()/1000:7.3f}ms  "
            + "  ".join(f"p{p}={q/1000:7.3f}ms" for p, q in zip(PERCENTILES, qs)))


# Categorical slots 1 and 2 of the validated default palette (CVD dE 24.7).
C = {"local": "#2a78d6", "global": "#eb6834"}
INK = "#0b0b0b"
MUTED = "#52514e"
GRID = "#d8d7d2"
# local is drawn thick + translucent, global thin and opaque on top: the two
# series track each other closely, so plain overlay hides one under the other.
WIDTHS = {"local": (3.4, 0.32), "global": (1.3, 1.0)}


def _style(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=11, color=INK, loc="left", pad=8)
    ax.set_xlabel(xlabel, fontsize=9, color=MUTED)
    ax.set_ylabel(ylabel, fontsize=9, color=MUTED)
    ax.tick_params(labelsize=8, colors=MUTED, length=3)
    ax.grid(alpha=0.35, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)


def _end_labels(ax, items, gap=0.055):
    """Direct-label series at their right edge, pushed apart so they never overlap.

    items: [(text, y_value, color)]. Placement is done in axes fractions, then
    converted back through the (possibly log/symlog) y transform.
    """
    if not items:
        return
    inv = ax.transAxes.inverted()
    fracs = []
    for text, yval, color in items:
        _, fy = inv.transform(ax.transData.transform((ax.get_xlim()[1], yval)))
        fracs.append([fy, text, color])
    fracs.sort(key=lambda r: r[0])
    for i in range(1, len(fracs)):                      # push up
        fracs[i][0] = max(fracs[i][0], fracs[i - 1][0] + gap)
    overflow = fracs[-1][0] - 1.0
    if overflow > 0:                                    # ...then back down if needed
        for r in fracs:
            r[0] -= overflow
    for fy, text, color in fracs:
        ax.annotate(text, xy=(1.0, min(max(fy, 0.0), 1.0)), xycoords="axes fraction",
                    xytext=(6, 0), textcoords="offset points", fontsize=8.5,
                    color=color, va="center", fontweight="bold", clip_on=False,
                    annotation_clip=False)


def _smooth(y, window):
    """Centred rolling median — keeps spikes honest while killing 1-bucket hair."""
    if window <= 1 or len(y) < window:
        return y
    pad = window // 2
    padded = np.pad(y, pad, mode="edge")
    return np.array([np.median(padded[i:i + window]) for i in range(len(y))])


def plot_file(path, args):
    parse.bucket_s = args.bucket
    buckets, lat_all, first, last, skipped, bad = parse(path, args.warmup)
    dur = (last - first) / US
    total = len(lat_all["local"]) + len(lat_all["global"])

    print(f"{path}: {total:,} msgs over {dur:.1f}s "
          f"({total/max(dur,1e-9):,.0f} msg/s), warmup-skipped={skipped:,}, malformed={bad:,}")
    for k in ("local", "global"):
        print(summarise(k, lat_all[k]))

    series = {}
    for k in ("local", "global"):
        if buckets[k]:
            series[k] = pct_series(buckets[k], args.bucket)

    fig = plt.figure(figsize=(15, 12), facecolor="#fcfcfb")
    gs = fig.add_gridspec(3, 2, height_ratios=[1.3, 1, 1], hspace=0.45, wspace=0.26,
                          left=0.055, right=0.93, top=0.925, bottom=0.055)
    fig.suptitle(f"{os.path.basename(path)} — {total:,} msgs over {dur:.0f}s, "
                 f"{total/max(dur,1e-9):,.0f} msg/s "
                 f"(bucket {args.bucket}s · first {args.warmup:g}s dropped)",
                 fontsize=13, color=INK, x=0.055, ha="left", y=0.975)

    # ---- 1. latency over time. Log y so a 4ms median and a 1.7s stall coexist.
    # No shaded band: two translucent fills over each other just make mud.
    ax = fig.add_subplot(gs[0, :])
    labels = []
    for k, (t, pcts, mean, _) in series.items():
        lw, alpha = WIDTHS[k]
        ax.plot(t, pcts[99], color=C[k], lw=lw * 0.55, alpha=alpha, ls=(0, (5, 2)))
        ax.plot(t, pcts[50], color=C[k], lw=lw, alpha=alpha)
        labels += [(f"{k} p99", pcts[99][-1], C[k]), (f"{k} p50", pcts[50][-1], C[k])]
    ax.set_yscale("log")
    _style(ax, "Latency over time — p50 (solid) vs p99 (dashed)",
           "time (s)", "latency (ms, log)")
    ax.set_xlim(0, None)
    _end_labels(ax, labels)

    # worst buckets called out inline, spread apart in time so the callouts
    # do not stack on top of each other
    if "global" in series:
        t, pcts, _, _ = series["global"]
        picked = []
        for i in np.argsort(pcts[99])[::-1]:
            if all(abs(t[i] - t[j]) > dur * 0.06 for j in picked):
                picked.append(i)
            if len(picked) == 3:
                break
        for n, i in enumerate(sorted(picked)):
            ax.annotate(f"{pcts[99][i]:.0f} ms @ {t[i]:.0f}s",
                        xy=(t[i], pcts[99][i]), xytext=(0, 14 + 12 * (n % 2)),
                        textcoords="offset points", fontsize=8, color=INK, ha="center",
                        arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.7))
    ax.legend(handles=[
        plt.Line2D([], [], color=C["local"], lw=3.4, alpha=0.32, label="local (1 dest)"),
        plt.Line2D([], [], color=C["global"], lw=1.3, label="global (multi dest)"),
    ], fontsize=9, frameon=False, loc="upper left")

    # ---- 2. throughput over time
    ax = fig.add_subplot(gs[1, 0])
    labels, tot_t, tot_r = [], None, None
    for k, (t, _, _, rate) in series.items():
        lw, alpha = WIDTHS[k]
        ax.plot(t, rate, color=C[k], lw=lw, alpha=alpha)
        labels.append((k, rate[-1], C[k]))
        if tot_t is None:
            tot_t, tot_r = t, rate.copy()
        elif len(t) == len(tot_t):
            tot_r = tot_r + rate
    if tot_r is not None and len(series) > 1:
        ax.plot(tot_t, tot_r, color=MUTED, lw=1.0)
        labels.append(("total", tot_r[-1], MUTED))
    _style(ax, "Throughput", "time (s)", "msgs/s")
    ax.set_ylim(0, None)
    ax.set_xlim(0, None)
    _end_labels(ax, labels)

    # ---- 3. the actual local-vs-global question as ONE series instead of two
    # near-identical overlapping ones: what does a multi-destination message cost?
    ax = fig.add_subplot(gs[1, 1])
    if len(series) == 2:
        tl, pl, _, _ = series["local"]
        tg, pg, _, _ = series["global"]
        n = min(len(tl), len(tg))
        labels = []
        for p, lw, ls, alpha in ((99, 1.0, (0, (5, 2)), 0.55), (50, 1.8, "-", 1.0)):
            d = _smooth(pg[p][:n] - pl[p][:n], args.smooth)
            ax.plot(tl[:n], d, color=C["global"], lw=lw, ls=ls, alpha=alpha)
            labels.append((f"p{p}", d[-1], C["global"]))
        ax.axhline(0, color=MUTED, lw=0.9)
        ax.set_yscale("symlog", linthresh=1)
        _end_labels(ax, labels)
    _style(ax, f"Global − local latency (rolling median, {args.smooth} buckets)",
           "time (s)", "delta (ms, symlog)")
    ax.set_xlim(0, None)

    # ---- 4. CCDF on log-log. A plain CDF squashes the tail into a flat line,
    # and the tail is the whole story here.
    ax = fig.add_subplot(gs[2, 0])
    for k in ("local", "global"):
        a = np.frombuffer(lat_all[k], dtype=np.int32)
        if a.size == 0:
            continue
        s = np.sort(a if a.size <= args.cdf_sample
                    else np.random.default_rng(0).choice(a, args.cdf_sample, replace=False))
        y = 1.0 - np.arange(s.size) / s.size
        lw, alpha = WIDTHS[k]
        ax.plot(s / 1000.0, y, color=C[k], lw=lw, alpha=alpha)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylim(1e-4, 1.2)
    _style(ax, "Tail distribution — share of msgs slower than x",
           "latency (ms, log)", "P(latency > x), log")
    x0 = ax.get_xlim()[0]
    for q, lbl in ((0.5, "p50"), (0.01, "p99"), (0.001, "p99.9")):
        ax.axhline(q, color=GRID, lw=0.8, ls=":")
        ax.annotate(lbl, xy=(x0, q), xytext=(3, 3), textcoords="offset points",
                    fontsize=7.5, color=MUTED, va="bottom", ha="left")
    ax.legend(handles=[plt.Line2D([], [], color=C[k], lw=w[0], alpha=max(w[1], 0.5),
                                  label=k) for k, w in WIDTHS.items()],
              fontsize=9, frameon=False, loc="lower left")

    # ---- 5. the numbers, as a table rather than a shape to squint at
    ax = fig.add_subplot(gs[2, 1])
    ax.axis("off")
    ax.set_title("Latency summary (ms)", fontsize=11, color=INK, loc="left", pad=8)
    rows, colors = [], []
    for k in ("local", "global"):
        a = np.frombuffer(lat_all[k], dtype=np.int32)
        if a.size == 0:
            continue
        qs = np.percentile(a, PERCENTILES)
        rows.append([k, f"{a.size:,}", f"{a.mean()/1000:.2f}"]
                    + [f"{q/1000:.2f}" for q in qs] + [f"{a.max()/1000:.0f}"])
        colors.append(C[k])
    tbl = ax.table(cellText=rows,
                   colLabels=["", "count", "mean"] + [f"p{p}" for p in PERCENTILES] + ["max"],
                   cellLoc="right", bbox=[0, 0.62, 1, 0.32])
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor(GRID)
        cell.set_linewidth(0.6)
        if r == 0:
            cell.set_text_props(color=MUTED, fontweight="bold")
        elif c == 0:
            cell.set_text_props(color=colors[r - 1], fontweight="bold", ha="left")
        else:
            cell.set_text_props(color=INK)

    os.makedirs(args.out, exist_ok=True)
    dest = os.path.join(args.out, os.path.basename(path).replace(".", "_") + ".png")
    fig.savefig(dest, dpi=130, facecolor="#fcfcfb")
    plt.close(fig)
    print(f"  -> {dest}")
    return dest


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="client.out.NN files")
    ap.add_argument("--bucket", type=float, default=1.0, help="time bucket seconds (default 1)")
    ap.add_argument("--warmup", type=float, default=5.0, help="seconds to drop at start (default 5)")
    ap.add_argument("--out", default="plots", help="output directory (default plots/)")
    ap.add_argument("--max-latency-ms", type=float, default=None,
                    help="clamp latency axes, e.g. 20; default autoscale to p99.9")
    ap.add_argument("--smooth", type=int, default=5,
                    help="rolling-median window (buckets) for the delta panel (default 5)")
    ap.add_argument("--cdf-sample", type=int, default=500_000,
                    help="max points sampled for the CDF (default 500k)")
    args = ap.parse_args()
    for path in args.files:
        plot_file(path, args)


if __name__ == "__main__":
    main()
