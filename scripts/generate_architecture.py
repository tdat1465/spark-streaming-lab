"""Generate a simple architecture diagram PNG without Graphviz."""

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "images" / "architecture.png"


def box(ax, xy, text, color="#E8F0FE"):
    x, y = xy
    patch = FancyBboxPatch(
        (x, y),
        2.4,
        0.9,
        boxstyle="round,pad=0.05,rounding_size=0.1",
        linewidth=1.2,
        edgecolor="#1A73E8",
        facecolor=color,
    )
    ax.add_patch(patch)
    ax.text(x + 1.2, y + 0.45, text, ha="center", va="center", fontsize=9, wrap=True)


def arrow(ax, start, end):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=12,
            linewidth=1.2,
            color="#444444",
        )
    )


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 10)
    ax.axis("off")
    ax.set_title(
        "Architecture: End-to-End CPG Streaming Pipeline",
        fontsize=14,
        fontweight="bold",
        color="#1A73E8",
        pad=16,
    )

    box(ax, (4.8, 8.6), "huggingface/peft\n(Python source)")
    box(ax, (4.8, 7.2), "File Discovery\n(Task 1)")
    box(ax, (4.8, 5.8), "CPG Parser Service\n(Task 2)")
    box(ax, (4.8, 4.4), "Apache Kafka\n(Task 3)")

    box(ax, (1.0, 2.6), "cpg-nodes / cpg-edges")
    box(ax, (4.8, 2.6), "cpg-metadata")
    box(ax, (8.6, 2.6), "cpg-errors")

    box(ax, (0.4, 0.8), "Neo4j\nKafka Connect\n(Task 4)", "#E6F4EA")
    box(ax, (4.4, 0.8), "Spark Structured\nStreaming (Task 5)", "#FEF7E0")
    box(ax, (8.4, 0.8), "Monitoring", "#FCE8E6")

    for y1, y2 in [(8.6, 7.2), (7.2, 5.8), (5.8, 4.4)]:
        arrow(ax, (6.0, y1), (6.0, y2 + 0.9))
    arrow(ax, (5.4, 4.4), (2.2, 3.5))
    arrow(ax, (6.0, 4.4), (6.0, 3.5))
    arrow(ax, (6.6, 4.4), (9.8, 3.5))
    arrow(ax, (2.2, 2.6), (1.6, 1.7))
    arrow(ax, (6.0, 2.6), (5.6, 1.7))
    arrow(ax, (9.8, 2.6), (9.4, 1.7))

    ax.text(
        6.0,
        0.15,
        "Task 6: mutate -> replay -> verify idempotent",
        ha="center",
        fontsize=9,
        style="italic",
        color="#555555",
    )

    fig.tight_layout()
    fig.savefig(OUT, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
