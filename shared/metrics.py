from collections import Counter

_metrics = Counter()


def increment(metric: str) -> None:
    _metrics[metric] += 1


def render_metrics() -> str:
    return "\n".join(f"payment_platform_{name} {value}" for name, value in sorted(_metrics.items())) + "\n"
