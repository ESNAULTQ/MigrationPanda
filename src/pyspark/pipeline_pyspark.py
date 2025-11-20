from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def load_settings(path: str | Path = "settings.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def controle_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}


def to_date(value) -> str:
    text = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Format de date non reconnu pour `{value}`.")


def _spark_to_sqlite_type(data_type: T.DataType) -> str:
    if isinstance(data_type, (T.IntegerType, T.LongType, T.ShortType)):
        return "INTEGER"
    if isinstance(data_type, (T.FloatType, T.DoubleType, T.DecimalType)):
        return "REAL"
    return "TEXT"


def _row_to_tuple(row, schema: T.StructType) -> tuple:
    values = []
    for field in schema.fields:
        val = row[field.name]
        if isinstance(val, Decimal):
            val = float(val)
        elif isinstance(val, bool):
            val = int(val)
        values.append(val)
    return tuple(values)


def _write_dataframe_to_sqlite(df: DataFrame, table: str, db_path: Path) -> None:
    rows = df.collect()
    schema = df.schema
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        col_defs = ", ".join(
            f"{field.name} {_spark_to_sqlite_type(field.dataType)}" for field in schema.fields
        )
        conn.execute(f"CREATE TABLE {table} ({col_defs})")
        if rows:
            placeholders = ",".join("?" for _ in schema.fields)
            sql = f"INSERT INTO {table} VALUES ({placeholders})"
            conn.executemany(sql, (_row_to_tuple(row, schema) for row in rows))
        conn.commit()


def _write_rows_to_csv(
    rows: Sequence[dict],
    columns: Sequence[str],
    path: Path,
    sep: str,
    encoding: str,
    float_format: str,
    float_cols: set[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding=encoding, newline="") as handle:
        writer = csv.writer(handle, delimiter=sep)
        writer.writerow(columns)
        for row in rows:
            output_row = []
            for col in columns:
                value = row.get(col)
                if value is None:
                    output_row.append("")
                    continue
                if col in float_cols:
                    try:
                        output_row.append(float_format % float(value))
                        continue
                    except (TypeError, ValueError):
                        pass
                if isinstance(value, Decimal):
                    value = float(value)
                output_row.append(value)
            writer.writerow(output_row)


def _write_daily_csvs(
    rows: Sequence[dict],
    output_dir: Path,
    sep: str,
    encoding: str,
    float_format: str,
) -> None:
    columns = [
        "date",
        "city",
        "channel",
        "orders_count",
        "unique_customers",
        "items_sold",
        "gross_revenue_eur",
        "refunds_eur",
        "net_revenue_eur",
    ]
    float_cols = {"gross_revenue_eur", "refunds_eur", "net_revenue_eur"}
    sorted_rows = sorted(
        rows,
        key=lambda r: (
            r.get("date") or "",
            r.get("city") or "",
            r.get("channel") or "",
        ),
    )
    all_path = output_dir / "daily_summary_all.csv"
    _write_rows_to_csv(sorted_rows, columns, all_path, sep, encoding, float_format, float_cols)
    by_date = defaultdict(list)
    for row in sorted_rows:
        by_date[row.get("date")].append(row)
    for date_value, subset in by_date.items():
        if not date_value:
            continue
        token = date_value.replace("-", "")
        day_path = output_dir / f"daily_summary_{token}.csv"
        _write_rows_to_csv(subset, columns, day_path, sep, encoding, float_format, float_cols)


def run_pipeline(settings_path: str | Path = "settings.yaml") -> list[dict]:
    cfg = load_settings(settings_path)
    input_dir = Path(cfg.get("input_dir", "./data/march-input"))
    output_dir = Path(cfg.get("output_dir", "./data/out"))
    db_path = Path(cfg.get("db_path", "./data/sales_db.db"))
    sep = cfg.get("csv_sep", ";")
    encoding = cfg.get("csv_encoding", "utf-8")
    float_format = cfg.get("csv_float_format", "%.2f")

    output_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    customers_path = input_dir / "customers.csv"
    refunds_path = input_dir / "refunds.csv"
    order_paths = sorted(input_dir.glob("orders_2025-03-*.json"))

    if not customers_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {customers_path}")
    if not refunds_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {refunds_path}")
    if not order_paths:
        raise FileNotFoundError("Aucun fichier orders_2025-03-XX.json trouvé.")

    spark = (
        SparkSession.builder.appName("MigrationPandaSpark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bool_udf = F.udf(controle_bool, T.BooleanType())
    order_date_udf = F.udf(to_date, T.StringType())

    try:
        customers_df = (
            spark.read.option("header", True).csv(str(customers_path))
            .withColumn("is_active", bool_udf(F.col("is_active")))
            .withColumn("customer_id", F.col("customer_id").cast("string"))
            .withColumn("city", F.col("city").cast("string"))
        )

        refunds_df = (
            spark.read.option("header", True).csv(str(refunds_path))
            .withColumn("order_id", F.col("order_id").cast("string"))
            .withColumn(
                "amount",
                F.coalesce(F.expr("try_cast(amount as double)"), F.lit(0.0)),
            )
            .withColumn("created_at", F.col("created_at").cast("string"))
        )

        orders_df = spark.read.option("multiline", True).json([str(path) for path in order_paths])
        paid_orders = orders_df.filter(F.lower(F.col("payment_status")) == "paid")
        exploded = paid_orders.withColumn("item", F.explode("items"))
        orders_items = (
            exploded
            .withColumn("item_qty_raw", F.col("item.qty"))
            .withColumn("item_unit_price_raw", F.col("item.unit_price"))
            .withColumn("item_sku", F.col("item.sku"))
            .drop("item")
        )

        required_cols = {"item_qty_raw", "item_unit_price_raw"}
        missing = [col for col in required_cols if col not in orders_items.columns]
        if missing:
            raise KeyError(f"Colonnes manquantes après explosion: {missing}")

        orders_items = (
            orders_items.withColumn(
                "item_qty",
                F.coalesce(F.expr("try_cast(item_qty_raw as int)"), F.lit(0)),
            )
            .withColumn(
                "item_unit_price",
                F.coalesce(F.expr("try_cast(item_unit_price_raw as double)"), F.lit(0.0)),
            )
            .drop("item_qty_raw", "item_unit_price_raw")
        )

        negative_items = orders_items.filter(F.col("item_unit_price") < 0)
        neg_rows = [row.asDict(recursive=False) for row in negative_items.collect()]
        if neg_rows:
            rejects_path = output_dir / "rejects_items.csv"
            _write_rows_to_csv(
                neg_rows,
                negative_items.columns,
                rejects_path,
                sep,
                encoding,
                float_format,
                float_cols={"item_unit_price"},
            )
        orders_clean = orders_items.filter(F.col("item_unit_price") >= 0)

        window = Window.partitionBy("order_id").orderBy("created_at")
        orders_dedup = (
            orders_clean.withColumn("row_number", F.row_number().over(window))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        orders_dedup = orders_dedup.withColumn(
            "line_gross", F.col("item_qty").cast("double") * F.col("item_unit_price")
        )

        per_order = orders_dedup.groupBy(
            "order_id", "customer_id", "channel", "created_at"
        ).agg(
            F.sum("item_qty").alias("items_sold"),
            F.sum("line_gross").alias("gross_revenue_eur"),
        )

        per_order = per_order.join(
            customers_df.select("customer_id", "city", "is_active"),
            on="customer_id",
            how="left",
        ).filter(F.col("is_active") == True)  # noqa: E712

        per_order = per_order.withColumn("order_date", order_date_udf(F.col("created_at")))

        refunds_sum = refunds_df.groupBy("order_id").agg(
            F.sum("amount").alias("refunds_eur")
        )
        per_order = per_order.join(refunds_sum, on="order_id", how="left").fillna({"refunds_eur": 0.0})

        per_order_save = per_order.select(
            "order_id",
            "customer_id",
            "city",
            "channel",
            "order_date",
            "items_sold",
            "gross_revenue_eur",
        )
        _write_dataframe_to_sqlite(per_order_save, "orders_clean", db_path)

        agg_df = (
            per_order.groupBy("order_date", "city", "channel")
            .agg(
                F.countDistinct("order_id").alias("orders_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("items_sold").alias("items_sold"),
                F.sum("gross_revenue_eur").alias("gross_revenue_eur"),
                F.sum("refunds_eur").alias("refunds_eur"),
            )
            .withColumn("net_revenue_eur", F.col("gross_revenue_eur") + F.col("refunds_eur"))
            .withColumnRenamed("order_date", "date")
            .orderBy("date", "city", "channel")
        )
        _write_dataframe_to_sqlite(agg_df, "daily_city_sales", db_path)

        agg_rows = [row.asDict(recursive=False) for row in agg_df.collect()]
        _write_daily_csvs(agg_rows, output_dir, sep, encoding, float_format)
        return agg_rows
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
