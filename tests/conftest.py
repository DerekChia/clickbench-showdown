"""Shared pytest configuration and fixtures for ClickBench Showdown tests."""


def pytest_addoption(parser):
    parser.addoption(
        "--reference-db",
        action="store",
        default="clickhouse",
        help="Database to use as reference for correctness comparison (default: clickhouse)",
    )
    parser.addoption(
        "--query-timeout",
        action="store",
        type=int,
        default=30,
        help="Timeout in seconds per query (default: 30)",
    )
