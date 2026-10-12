"""Brazilian retail demand curve: month, weekday, commercial dates and growth."""

from datetime import date, datetime, timedelta, timezone
from random import Random
from typing import Final

BRASILIA: Final = timezone(timedelta(hours=-3), "BRT")  # no daylight saving since 2019

MONTH_FACTORS: Final = {
    1: 0.80, 2: 0.85, 3: 0.95, 4: 0.95, 5: 1.10, 6: 1.00,
    7: 1.00, 8: 1.05, 9: 0.95, 10: 1.05, 11: 1.35, 12: 1.50,
}  # fmt: skip
WEEKDAY_FACTORS: Final = (0.90, 0.90, 0.95, 1.00, 1.15, 1.25, 0.95)  # Monday..Sunday
HOUR_WEIGHTS: Final = (
    0.2, 0.1, 0.1, 0.1, 0.1, 0.2, 0.4, 0.7, 1.0, 1.4, 1.8, 2.0,
    2.4, 2.4, 2.0, 1.8, 1.8, 2.0, 2.4, 2.6, 2.4, 1.8, 1.0, 0.5,
)  # fmt: skip
ANNUAL_GROWTH: Final = 0.12
DAYS_PER_YEAR: Final = 365.0
MINUTES, SECONDS = 60, 60

BLACK_FRIDAY_FACTOR: Final = 4.0
BLACK_WEEKEND_FACTOR: Final = 2.0  # Saturday to Cyber Monday
BLACK_WEEK_FACTOR: Final = 1.5  # the six days before Black Friday
CHRISTMAS_RUSH_FACTOR: Final = 1.4  # 15-23 December
MOTHERS_DAY_WEEK_FACTOR: Final = 1.3
HOLIDAY_FACTOR: Final = 0.3  # 25 December and 1 January


def black_friday(year: int) -> date:
    """The Friday after the fourth Thursday of November (not simply the fourth Friday)."""
    first = date(year, 11, 1)
    first_thursday = first + timedelta(days=(3 - first.weekday()) % 7)
    return first_thursday + timedelta(weeks=3, days=1)


def mothers_day(year: int) -> date:
    """Second Sunday of May (Brazil)."""
    first = date(year, 5, 1)
    first_sunday = first + timedelta(days=(6 - first.weekday()) % 7)
    return first_sunday + timedelta(weeks=1)


def _special_factor(day: date) -> float:  # noqa: PLR0911 - one branch per commercial date
    friday = black_friday(day.year)
    if day == friday:
        return BLACK_FRIDAY_FACTOR
    if friday < day <= friday + timedelta(days=3):
        return BLACK_WEEKEND_FACTOR
    if friday - timedelta(days=6) <= day < friday:
        return BLACK_WEEK_FACTOR
    if (day.month, day.day) in {(12, 25), (1, 1)}:
        return HOLIDAY_FACTOR
    if day.month == 12 and 15 <= day.day <= 23:  # noqa: PLR2004 - Christmas rush window
        return CHRISTMAS_RUSH_FACTOR
    mother = mothers_day(day.year)
    if mother - timedelta(days=7) <= day < mother:
        return MOTHERS_DAY_WEEK_FACTOR
    return 1.0


def daily_weights(start: date, end: date) -> dict[date, float]:
    """Relative demand for every day in [start, end]."""
    weights = {}
    day = start
    while day <= end:
        growth = 1 + ANNUAL_GROWTH * (day - start).days / DAYS_PER_YEAR
        weights[day] = (
            MONTH_FACTORS[day.month]
            * WEEKDAY_FACTORS[day.weekday()]
            * _special_factor(day)
            * growth
        )
        day += timedelta(days=1)
    return weights


def promotion(day: date) -> tuple[float, float, float]:
    """(probability of an item being on sale, min discount, max discount) for a day."""
    friday = black_friday(day.year)
    if friday - timedelta(days=6) <= day <= friday + timedelta(days=3):
        return 0.6, 0.15, 0.40
    if day.month == 11:  # noqa: PLR2004 - November
        return 0.3, 0.05, 0.15
    if day.month == 12:  # noqa: PLR2004 - December
        return 0.25, 0.05, 0.10
    return 0.08, 0.05, 0.10


def sample_order_timestamps(rng: Random, start: date, end: date, count: int) -> list[datetime]:
    """``count`` sorted order timestamps following the demand curve, in Brasília time."""
    weights = daily_weights(start, end)
    days = rng.choices(list(weights), weights=list(weights.values()), k=count)
    hours = rng.choices(range(len(HOUR_WEIGHTS)), weights=HOUR_WEIGHTS, k=count)
    timestamps = [
        datetime(
            day.year,
            day.month,
            day.day,
            hour,
            rng.randrange(MINUTES),
            rng.randrange(SECONDS),
            tzinfo=BRASILIA,
        )
        for day, hour in zip(days, hours, strict=True)
    ]
    return sorted(timestamps)
