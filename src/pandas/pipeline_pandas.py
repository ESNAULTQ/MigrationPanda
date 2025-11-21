from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

import yaml

import pandas as pd


def load_settings(path: str | Path = "settings.yaml") -> dict:
    """Charge le fichier YAML de configuration."""
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def controle_bool(value) -> bool:
    """Convertit une colonne hétérogène (bool, int, str) en booléen."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}


def to_date(value: str) -> str:
    """Retourne la date (YYYY-MM-DD) à partir d'un timestamp."""
    text = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Format de date non reconnu pour `{value}`.")


def _load_orders(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        if not path.exists():
            continue
        frames.append(pd.read_json(path))
    if not frames:
        raise FileNotFoundError("Aucun fichier d'orders JSON trouvé.")
    return pd.concat(frames, ignore_index=True)


def _prepare_customers(path: Path) -> pd.DataFrame:
    customers = pd.read_csv(path)
    customers["is_active"] = customers["is_active"].apply(controle_bool)
    return customers.astype({"customer_id": "string", "city": "string"})


def _prepare_refunds(path: Path) -> pd.DataFrame:
    refunds = pd.read_csv(path)
    refunds["amount"] = pd.to_numeric(refunds["amount"], errors="coerce").fillna(0.0)
    refunds["created_at"] = refunds["created_at"].astype("string")
    return refunds


def _explode_items(orders: pd.DataFrame) -> pd.DataFrame:
    exploded = orders.explode("items", ignore_index=True)
    items = pd.json_normalize(exploded["items"]).add_prefix("item_")
    return pd.concat([exploded.drop(columns=["items"]), items], axis=1)


def run_pipeline(settings_path: str | Path = "settings.yaml") -> pd.DataFrame:
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

    customers = _prepare_customers(customers_path)
    refunds = _prepare_refunds(refunds_path)
    orders = _load_orders(order_paths)

    paid_mask = orders["payment_status"].fillna("").str.lower() == "paid"
    orders = orders.loc[paid_mask].copy()

    orders2 = _explode_items(orders)
    for col in ("item_qty", "item_unit_price"):
        if col not in orders2.columns:
            raise KeyError(f"Colonne `{col}` absente après explosion des items.")
    orders2["item_qty"] = pd.to_numeric(orders2["item_qty"], errors="coerce").fillna(0)
    orders2["item_unit_price"] = pd.to_numeric(
        orders2["item_unit_price"], errors="coerce"
    ).fillna(0.0)

    # Sauvegarde des lignes rejetées
    neg_mask = orders2["item_unit_price"] < 0
    if neg_mask.any():
        rejects_path = output_dir / "rejects_items.csv"
        orders2.loc[neg_mask].to_csv(rejects_path, index=False, encoding=encoding)
        orders2 = orders2.loc[~neg_mask].copy()

    orders3 = (
        orders2.sort_values(["order_id", "created_at"])
        .drop_duplicates(subset=["order_id"], keep="first")
        .copy()
    )
    orders3["line_gross"] = orders3["item_qty"] * orders3["item_unit_price"]

    per_order = orders3.groupby(
        ["order_id", "customer_id", "channel", "created_at"], as_index=False
    ).agg(
        items_sold=("item_qty", "sum"),
        gross_revenue_eur=("line_gross", "sum"),
    )

    per_order = per_order.merge(
        customers[["customer_id", "city", "is_active"]], on="customer_id", how="left"
    )
    per_order = per_order.loc[per_order["is_active"] == True].copy()  # noqa: E712
    per_order["order_date"] = per_order["created_at"].apply(to_date)

    if not refunds.empty:
        refunds_sum = (
            refunds.groupby("order_id", as_index=False)["amount"]
            .sum()
            .rename(columns={"amount": "refunds_eur"})
        )
        per_order = per_order.merge(refunds_sum, on="order_id", how="left")
    else:
        per_order["refunds_eur"] = 0.0
    per_order["refunds_eur"] = per_order["refunds_eur"].fillna(0.0)

    per_order_save = per_order[
        [
            "order_id",
            "customer_id",
            "city",
            "channel",
            "order_date",
            "items_sold",
            "gross_revenue_eur",
        ]
    ].copy()

    with sqlite3.connect(db_path) as conn:
        per_order_save.to_sql("orders_clean", conn, if_exists="replace", index=False)

    agg = per_order.groupby(["order_date", "city", "channel"], as_index=False).agg(
        orders_count=("order_id", "nunique"),
        unique_customers=("customer_id", "nunique"),
        items_sold=("items_sold", "sum"),
        gross_revenue_eur=("gross_revenue_eur", "sum"),
        refunds_eur=("refunds_eur", "sum"),
    )
    agg["net_revenue_eur"] = agg["gross_revenue_eur"] + agg["refunds_eur"]
    agg = (
        agg.rename(columns={"order_date": "date"})
        .sort_values(["date", "city", "channel"])
        .reset_index(drop=True)
    )

    with sqlite3.connect(db_path) as conn:
        agg.to_sql("daily_city_sales", conn, if_exists="replace", index=False)

    for date_value, subset in agg.groupby("date"):
        out_path = output_dir / f"daily_summary_{date_value.replace('-', '')}.csv"
        subset[
            [
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
        ].to_csv(
            out_path,
            index=False,
            sep=sep,
            encoding=encoding,
            float_format=float_format,
        )

    all_path = output_dir / "daily_summary_all.csv"
    agg.to_csv(
        all_path, index=False, sep=sep, encoding=encoding, float_format=float_format
    )
    return agg


if __name__ == "__main__":
    run_pipeline()
