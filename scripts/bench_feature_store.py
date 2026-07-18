"""
Measure the feature store retrieval latency (at p50 / p95) for the report.
"""

import statistics
import time
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger
from tqdm import tqdm

from src.streaming.feature_store import FeatureStore

NUM_TESTS = 1000
CUSTOMER_ID = "BENCH0001"
FEATURES = {"transaction_count": 5, "avg_amount": 120.0}
LOG_DIR = Path(__file__).parent.parent / "logs"

app = typer.Typer(add_completion=False)


# ---- Helpers -----------------------------------------------------------------


def _setup_logging(verbose: bool) -> None:
    logger.remove()
    logger.add(
        # Route loguru through tqdm so progress bars aren't interrupted by logs
        lambda msg: tqdm.write(msg, end=""),
        level=("DEBUG" if verbose else "INFO"),
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>  "
            "<level>{message}</level>"
        ),
    )
    LOG_DIR.mkdir(exist_ok=True)
    logger.add(LOG_DIR / "{time}_bench_feature_store.log")


def _setup_store_with_features() -> FeatureStore:
    logger.debug("Initializing feature store...")
    store = FeatureStore()
    logger.debug(f"Adding features to store for customer: '{CUSTOMER_ID}'")
    store.store_customer_features(CUSTOMER_ID, FEATURES)
    logger.debug("Store setup complete.")
    return store

def _teardown_feature_store(store: FeatureStore) -> None:
    logger.debug("Starting feature store teardown")
    logger.debug(f"Removing customer from store: '{CUSTOMER_ID}'")
    store.client.delete(store._key(CUSTOMER_ID))
    logger.debug("Feature store teardown complete.")


def _run_tests(store: FeatureStore, num_tests: int) -> list[float]:
    """Runs the feature store tests and returns a list of latencies in ms."""
    logger.debug(f"Starting tests (total: {num_tests:,})")
    latencies_ms: list[float] = []

    for i in tqdm(range(1, num_tests+1), "Running tests", total=num_tests):
        start_time = time.perf_counter()
        store.get_customer_features(CUSTOMER_ID)
        duration_ms = 1000 * (time.perf_counter() - start_time)
        latencies_ms.append(duration_ms)
        logger.debug(f"Test {i:04}/{num_tests:04}: {duration_ms:,.3f}ms")
    
    return latencies_ms


def _percentile(values: list[float], p: float) -> float:
    if not (0 <= p <= 1):
        raise ValueError(f"Invalid value of p: {p}")
    index = round(p * len(values)) - 1
    return values[index]

def _summarize_latencies(latencies: list[float]) -> dict[str, int]:
    latencies = sorted(latencies)
    summary = {}

    summary["count"] = len(latencies)
    summary["mean"] = round(statistics.mean(latencies), 3)
    summary["std"] = round(statistics.stdev(latencies), 3)
    summary["min"] = round(min(latencies), 3)
    summary["p25"] = round(_percentile(latencies, p=0.25), 3)
    summary["p50"] = round(_percentile(latencies, p=0.50), 3)
    summary["p75"] = round(_percentile(latencies, p=0.75), 3)
    summary["p90"] = round(_percentile(latencies, p=0.90), 3)
    summary["p95"] = round(_percentile(latencies, p=0.95), 3)
    summary["p99"] = round(_percentile(latencies, p=0.99), 3)
    summary["max"] = round(max(latencies), 3)
    
    logger.debug(f"Latency summary: {summary}")
    return summary


# ---- Entry Point -------------------------------------------------------------


@app.command()
def main(
        num_tests: Annotated[int, typer.Option(
            "-n", "--num-tests",
            help="Number of tests to run.",
        )] = NUM_TESTS,
        verbose: Annotated[bool, typer.Option(
            "-v", "--verbose",
            help="Set log level to DEBUG",
        )] = False,
) -> None:
    _setup_logging(verbose=verbose)
    logger.info("Starting feature store benchmark")
    store = _setup_store_with_features()
    latencies_ms = _run_tests(store, num_tests)
    summary = _summarize_latencies(latencies_ms)
    
    logger.info("Tests complete. Summary of results:")
    print()
    for field, value in summary.items():
        if field == "count":
            print(f"{field:<9}  {value: 7.0f}")
        else:
            print(f"{field:<9}  {value: 7.2f}ms")
    print()

    _teardown_feature_store(store)
    logger.success("Feature store benchmark complete.")
    return


if __name__ == "__main__":
    app()