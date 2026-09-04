import sys

import pytest


def main() -> int:
    """Lance les tests PySpark depuis spark-submit."""

    return pytest.main(
        [
            "/home/jovyan/tests/test_transformers.py",
            "-v",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())