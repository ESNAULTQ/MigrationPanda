from __future__ import annotations

from pathlib import Path
import sys

# S'assurer que le projet est dans sys.path pour exécutions directes
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.test_pipeline_equivalence import (
    run_equivalence_check,
    compare_dataframes,
)


def test_pipeline_equivalence(tmp_path):
    pandas_sorted, spark_sorted = run_equivalence_check(tmp_path, verbose=False)
    ok, issues = compare_dataframes(pandas_sorted, spark_sorted, verbose=False)
    assert ok, " ; ".join(issues)
