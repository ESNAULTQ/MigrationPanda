from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pandas.testing as pdt

from src.pandas.pipeline_pandas import run_pipeline as run_pandas_pipeline
from src.pyspark.pipeline_pyspark import run_pipeline as run_spark_pipeline


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header), *[",".join(map(str, row)) for row in rows]]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _create_dataset(base_dir: Path) -> None:
    input_dir = base_dir / "data" / "march-input"
    input_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        input_dir / "customers.csv",
        ["customer_id", "city", "is_active"],
        [
            ["CUST1", "Paris", "true"],
            ["CUST2", "Lyon", "false"],
            ["CUST3", "Marseille", "1"],
        ],
    )

    _write_csv(
        input_dir / "refunds.csv",
        ["order_id", "amount", "created_at"],
        [
            ["O1", "-2.50", "2025-03-03"],
            ["O3", "error", "2025-03-05"],
        ],
    )

    orders_day1 = [
        {
            "order_id": "O1",
            "customer_id": "CUST1",
            "channel": "online",
            "created_at": "2025-03-01 10:00:00",
            "payment_status": "paid",
            "items": [
                {"sku": "SKU1", "qty": 2, "unit_price": 10.0},
                {"sku": "SKU_NEG", "qty": 1, "unit_price": -5.0},
            ],
        },
        {
            "order_id": "O2",
            "customer_id": "CUST2",
            "channel": "store",
            "created_at": "2025-03-01 12:00:00",
            "payment_status": "pending",
            "items": [
                {"sku": "SKU3", "qty": 1, "unit_price": 15.0},
            ],
        },
    ]
    _write_json(input_dir / "orders_2025-03-01.json", orders_day1)

    orders_day2 = [
        {
            "order_id": "O3",
            "customer_id": "CUST3",
            "channel": "online",
            "created_at": "2025-03-02 09:30:00",
            "payment_status": "paid",
            "items": [
                {"sku": "SKU4", "qty": 3, "unit_price": "20.0"},
                {"sku": "SKU5", "qty": 1, "unit_price": "error"},
            ],
        },
        {
            "order_id": "O4",
            "customer_id": "CUST1",
            "channel": "store",
            "created_at": "2025-03-02 11:00:00",
            "payment_status": "paid",
            "items": [
                {"sku": "SKU6", "qty": 1, "unit_price": 30.0},
            ],
        },
    ]
    _write_json(input_dir / "orders_2025-03-02.json", orders_day2)


def _write_settings(path: Path, input_dir: Path, output_dir: Path, db_path: Path) -> None:
    content = (
        f"input_dir: {input_dir}\n"
        f"output_dir: {output_dir}\n"
        f"db_path: {db_path}\n"
        "csv_sep: ';'\n"
        "csv_encoding: 'utf-8'\n"
        "csv_float_format: '%.2f'\n"
    )
    path.write_text(content, encoding="utf-8")


def _sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["date", "city", "channel"]).reset_index(drop=True)


def run_equivalence_check(
    base_dir: Path, verbose: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    _create_dataset(base_dir)
    input_dir = base_dir / "data" / "march-input"
    if verbose:
        print(f"[equivalence-test] Jeu de données généré dans: {input_dir}")

    pandas_settings = base_dir / "settings_pandas.yaml"
    spark_settings = base_dir / "settings_spark.yaml"

    pandas_out = base_dir / "out_pandas"
    spark_out = base_dir / "out_spark"

    _write_settings(
        pandas_settings,
        input_dir=input_dir,
        output_dir=pandas_out,
        db_path=base_dir / "out_pandas" / "sales.db",
    )
    _write_settings(
        spark_settings,
        input_dir=input_dir,
        output_dir=spark_out,
        db_path=base_dir / "out_spark" / "sales.db",
    )

    if verbose:
        print(f"[equivalence-test] Lancement pipeline pandas avec {pandas_settings}")
    pandas_result = run_pandas_pipeline(pandas_settings)
    if verbose:
        print(
            "[equivalence-test] Pipeline pandas terminé -> "
            f"{len(pandas_result)} lignes agrégées."
        )
        print(f"[equivalence-test] Lancement pipeline Spark avec {spark_settings}")
    spark_rows = run_spark_pipeline(spark_settings)
    if verbose:
        print(
            "[equivalence-test] Pipeline Spark terminé -> "
            f"{len(spark_rows)} lignes agrégées."
        )
    spark_df = pd.DataFrame(spark_rows, columns=list(pandas_result.columns))

    pandas_sorted = _sort_dataframe(pandas_result)
    spark_sorted = _sort_dataframe(spark_df)

    if verbose:
        print("[equivalence-test] Comparaison des DataFrames triés…")
    return pandas_sorted.reset_index(drop=True), spark_sorted.reset_index(drop=True)


def compare_dataframes(
    pandas_df: pd.DataFrame, spark_df: pd.DataFrame, *, verbose: bool = True
) -> tuple[bool, list[str]]:
    issues: list[str] = []

    pandas_cols = list(pandas_df.columns)
    spark_cols = list(spark_df.columns)
    if pandas_cols != spark_cols:
        issues.append(
            "Colonnes différentes : "
            f"pandas={pandas_cols}, spark={spark_cols}"
        )

    if len(pandas_df) != len(spark_df):
        issues.append(
            f"Nombre de lignes différent : pandas={len(pandas_df)}, spark={len(spark_df)}"
        )

    pandas_dtypes = {col: str(dtype) for col, dtype in pandas_df.dtypes.items()}
    spark_dtypes = {col: str(dtype) for col, dtype in spark_df.dtypes.items()}
    common_cols = set(pandas_cols).intersection(spark_cols)
    for col in common_cols:
        if pandas_dtypes.get(col) != spark_dtypes.get(col):
            issue = (
                f"Dtype différent pour `{col}` : pandas={pandas_dtypes.get(col)} "
                f"vs spark={spark_dtypes.get(col)}"
            )
            # Tolérance pour string/object équivalents
            pandas_type = pandas_dtypes.get(col)
            spark_type = spark_dtypes.get(col)
            string_equiv = (
                pandas_type in {"string", "string[python]"}
                and spark_type == "object"
            )
            if string_equiv:
                if verbose:
                    print(f"[equivalence-test] ⚠︎ {issue} (toléré).")
                continue
            issues.append(issue)

    if not issues:
        try:
            pdt.assert_frame_equal(
                pandas_df,
                spark_df,
                check_exact=False,
                rtol=1e-6,
                atol=1e-9,
                check_dtype=False,
            )
        except AssertionError as err:
            issues.append(f"Différence de valeurs: {err}")

    if verbose:
        if issues:
            print("[equivalence-test] ❌ Écart détecté :")
            for issue in issues:
                print(f"  - {issue}")
        else:
            print("[equivalence-test] ✅ Aucun écart détecté (données équivalentes).")
    return len(issues) == 0, issues


def test_pandas_and_spark_pipelines_produce_same_results(tmp_path):
    pandas_sorted, spark_sorted = run_equivalence_check(tmp_path, verbose=False)
    ok, issues = compare_dataframes(pandas_sorted, spark_sorted, verbose=False)
    assert ok, " ; ".join(issues)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        pandas_sorted, spark_sorted = run_equivalence_check(base_dir, verbose=True)
        print("[equivalence-test] Vérification finale…")
        ok, issues = compare_dataframes(
            pandas_sorted, spark_sorted, verbose=True
        )
        if not ok:
            raise SystemExit(
                "[equivalence-test] ❌ Échec : "
                + " ; ".join(issues)
            )
        print(
            "[equivalence-test] ✅ Pipelines pandas et Spark produisent des résultats équivalents."
        )
