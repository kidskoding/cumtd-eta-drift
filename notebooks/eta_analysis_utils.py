"""Shared helpers for CUMTD ETA drift analysis notebooks."""

from __future__ import annotations

import math
from typing import Iterable

import pandas as pd

LOW_DRIFT_MIN = 2.0
MEDIUM_DRIFT_MIN = 5.0
HIGH_DRIFT_MIN = 10.0

COLORS = {
    "ink": "#1f2937",
    "muted": "#6b7280",
    "faint": "#e5e7eb",
    "bg": "#ffffff",
    "green": "#16a34a",
    "amber": "#f59e0b",
    "red": "#ef4444",
    "blue": "#2563eb",
    "purple": "#6366f1",
}


def route_risk_label(p90_drift: float) -> str:
    if p90_drift <= LOW_DRIFT_MIN:
        return "High trust"
    if p90_drift <= MEDIUM_DRIFT_MIN:
        return "Medium trust"
    return "Low trust"


def route_risk_color(p90_drift: float) -> str:
    if p90_drift <= LOW_DRIFT_MIN:
        return COLORS["green"]
    if p90_drift <= MEDIUM_DRIFT_MIN:
        return COLORS["amber"]
    return COLORS["red"]


def trust_score_from_p90(p90_drift: float) -> int:
    # Simple and explainable: p90 drift of 0 min -> 100, p90 drift of 10+ min -> 10.
    return int(max(10, min(100, round(100 - (p90_drift * 9)))))


def suggested_buffer_minutes(p90_drift: float) -> int:
    # Round up to a student-friendly buffer. This is not predicted lateness.
    return int(max(1, math.ceil(p90_drift)))


def stop_label(row: pd.Series | dict) -> str:
    values = row if isinstance(row, dict) else row.to_dict()
    return (
        values.get("stop_display_name")
        or values.get("stop_name")
        or values.get("stop_id")
        or "Unknown stop"
    )


def style_axis(ax, grid_axis: str = "x") -> None:
    ax.set_facecolor(COLORS["bg"])
    ax.tick_params(labelsize=10, colors=COLORS["muted"], length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis=grid_axis, alpha=0.35, color=COLORS["faint"], linewidth=0.8)


def add_bar_labels(ax, values: Iterable[float], suffix: str = " min") -> None:
    values = list(values)
    x_max = max(values) if values else 0
    for patch in ax.patches:
        width = patch.get_width()
        ax.text(
            width + max(x_max * 0.015, 0.08),
            patch.get_y() + patch.get_height() / 2,
            f"{width:.1f}{suffix}",
            va="center",
            fontsize=10,
            color=COLORS["ink"],
            fontweight="bold",
        )


def kpi_cards_html(cards: list[tuple[str, str, str]]) -> str:
    card_html = "".join(
        f"""
        <div style="border:1px solid #e5e7eb;border-radius:8px;padding:16px 18px;background:#ffffff;">
          <div style="font-size:13px;color:#6b7280;margin-bottom:8px;">{label}</div>
          <div style="font-size:30px;font-weight:800;color:#111827;line-height:1;">{value}</div>
          <div style="font-size:12px;color:#6b7280;margin-top:8px;">{note}</div>
        </div>
        """
        for label, value, note in cards
    )
    return f"""
    <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:12px 0 18px 0;">
      {card_html}
    </div>
    """
