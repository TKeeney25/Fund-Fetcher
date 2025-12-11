import logging
from database.query_processor import Processor
from database.models import Ticker
logger = logging.getLogger(__name__)

RETURN_FIELDS = [
    "return_ytd", "return_1y", "return_3y", "return_5y",
    "return_10y", "return_15y", "inception"
]

# SPECS
RETURN_YTD_SPEC_MAX = 0.5
RETURN_1Y_SPEC_MAX = 1.0
RETURN_3Y_SPEC_MAX = 5.0
RETURN_5Y_SPEC_MAX = 10.0
RETURN_10Y_SPEC_MAX = 15.0
RETURN_15Y_SPEC_MAX = 30.0
RETURN_INCEPTION_SPEC_MAX = 50.0

# Morningstar rating min and max specs
MORNINGSTAR_RATING_SPECS = {
    "None": {"min": 0.0, "max": 5.0},
    1: {"min": 1.0, "max": 5.0},
    2: {"min": 5.0, "max": 15.0},
    3: {"min": 20.0, "max": 40.0},
    4: {"min": 20.0, "max": 40.0},
    5: {"min": 10.0, "max": 20.0},
}

def check_data_controls(processor:Processor) -> list[str]:
    logger.info("Starting processor and fetching data...")
    failing_specs = []
    tickers: list[Ticker] = processor.get_everything()
    morningstar_ratings = {"None": 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

    # Initialize counters for returns
    return_none_counts = {field: 0 for field in RETURN_FIELDS}
    return_sums = {field: 0.0 for field in RETURN_FIELDS}
    return_counts = {field: 0 for field in RETURN_FIELDS}

    for ticker in tickers:
        rating = ticker.morningstar_rating
        if rating is None:
            morningstar_ratings["None"] += 1
        else:
            morningstar_ratings[rating] += 1

        # Count None and sum values for returns
        for field in RETURN_FIELDS:
            value = getattr(ticker, field, None)
            if value is None:
                return_none_counts[field] += 1
            else:
                return_sums[field] += value
                return_counts[field] += 1

    total_symbols = len(tickers)

    # Calculate averages
    return_averages = {
        field: (return_sums[field] / return_counts[field]) if return_counts[field] > 0 else None
        for field in RETURN_FIELDS
    }

    # Calculate percent of tickers for each star rating
    morningstar_percentages = {
        key: (count / total_symbols * 100) if total_symbols > 0 else 0
        for key, count in morningstar_ratings.items()
    }
    # Calculate percent of tickers that have each return field as None
    return_none_percentages = {
        field: (return_none_counts[field] / total_symbols * 100) if total_symbols > 0 else 0
        for field in RETURN_FIELDS
    }



    # Check return averages against their spec max values
    return_spec_max = {
        "return_ytd": RETURN_YTD_SPEC_MAX,
        "return_1y": RETURN_1Y_SPEC_MAX,
        "return_3y": RETURN_3Y_SPEC_MAX,
        "return_5y": RETURN_5Y_SPEC_MAX,
        "return_10y": RETURN_10Y_SPEC_MAX,
        "return_15y": RETURN_15Y_SPEC_MAX,
        "inception": RETURN_INCEPTION_SPEC_MAX,
    }
    for field, none_percent in return_none_percentages.items():
        if none_percent is not None and none_percent > return_spec_max[field]:
            failing_specs.append(f"{field} none percentage above spec: {none_percent:.2f} > {return_spec_max[field]}")

    # Check morningstar percentages against their min/max specs
    for rating, specs in MORNINGSTAR_RATING_SPECS.items():
        percent = morningstar_percentages.get(rating, 0)
        if percent < specs["min"] or percent > specs["max"]:
            failing_specs.append(
                f"Morningstar rating {rating} percentage out of spec: {percent:.2f}% (spec: {specs['min']}%-{specs['max']}%)"
            )

    logging.info(
        "Morningstar Ratings: %s\n"
        "Morningstar Percentages: %s\n"
        "Total Symbols: %s\n"
        "Return None Counts: %s\n"
        "Return None Percentages: %s\n"
        "Return Averages: %s\n"
        "Failing Specs: %s\n",
        morningstar_ratings,
        morningstar_percentages,
        total_symbols,
        return_none_counts,
        return_none_percentages,
        return_averages,
        failing_specs
    )
    return failing_specs
