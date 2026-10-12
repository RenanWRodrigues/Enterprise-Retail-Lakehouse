"""Seasonal demand curve and timestamp sampling."""

from collections import Counter
from datetime import date
from random import Random

from retail_lakehouse.synthetic.seasonality import (
    BRASILIA,
    black_friday,
    daily_weights,
    sample_order_timestamps,
)

START, END = date(2024, 1, 1), date(2025, 12, 31)


def test_black_friday_is_the_friday_after_the_fourth_thursday() -> None:
    # Regression: in 2024 November starts on a Friday, so the 4th Friday (22) is wrong.
    assert black_friday(2024) == date(2024, 11, 29)
    assert black_friday(2025) == date(2025, 11, 28)


def test_weights_cover_every_day_of_the_window() -> None:
    weights = daily_weights(START, END)
    assert len(weights) == (END - START).days + 1
    assert all(weight > 0 for weight in weights.values())


def test_black_friday_outweighs_an_ordinary_friday() -> None:
    weights = daily_weights(START, END)
    assert weights[date(2024, 11, 29)] > 2 * weights[date(2024, 11, 8)]


def test_december_outweighs_march_and_growth_is_positive() -> None:
    weights = daily_weights(START, END)

    def month_total(year: int, month: int) -> float:
        return sum(w for d, w in weights.items() if (d.year, d.month) == (year, month))

    assert month_total(2024, 12) > 1.3 * month_total(2024, 3)
    assert month_total(2025, 3) > month_total(2024, 3)


def test_sampled_timestamps_are_sorted_in_window_and_deterministic() -> None:
    first = sample_order_timestamps(Random(1), START, END, 5_000)
    second = sample_order_timestamps(Random(1), START, END, 5_000)

    assert first == second
    assert first == sorted(first)
    assert all(START <= ts.date() <= END and ts.tzinfo == BRASILIA for ts in first)


def test_sampled_timestamps_follow_seasonality() -> None:
    timestamps = sample_order_timestamps(Random(2), START, END, 20_000)
    per_month = Counter((ts.year, ts.month) for ts in timestamps)
    assert per_month[(2024, 11)] > per_month[(2024, 2)]
    assert per_month[(2025, 12)] > per_month[(2025, 4)]
