"""Market data retrieval client backed by Yahoo Finance (yfinance)."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import yfinance as yf


class FinanceClient:
    """Fetch company fundamentals and statements from Yahoo Finance."""

    def fetch_company_snapshot(self, ticker: str) -> Dict[str, Any]:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        return {
            "ticker": ticker,
            "info": {
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_sales_ttm": info.get("priceToSalesTrailing12Months"),
                "peg_ratio": info.get("pegRatio"),
                "gross_margins": info.get("grossMargins"),
                "operating_margins": info.get("operatingMargins"),
                "profit_margins": info.get("profitMargins"),
                "revenue_growth": info.get("revenueGrowth"),
                "earnings_growth": info.get("earningsGrowth"),
                "return_on_equity": info.get("returnOnEquity"),
                "return_on_assets": info.get("returnOnAssets"),
                "debt_to_equity": info.get("debtToEquity"),
                "current_ratio": info.get("currentRatio"),
                "quick_ratio": info.get("quickRatio"),
                "beta": info.get("beta"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                "currency": info.get("currency"),
                "exchange": info.get("exchange"),
                "long_business_summary": info.get("longBusinessSummary"),
            },
            "financial_statements": {
                "income_statement": _df_to_records(stock.financials),
                "balance_sheet": _df_to_records(stock.balance_sheet),
                "cash_flow": _df_to_records(stock.cashflow),
            },
        }


def _df_to_records(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Convert statement dataframe into JSON-safe nested dictionary."""
    if df is None or df.empty:
        return {}

    safe_df = df.copy()
    safe_df.columns = [str(col) for col in safe_df.columns]
    safe_df.index = [str(idx) for idx in safe_df.index]

    data = safe_df.to_dict()
    normalized: Dict[str, Dict[str, Any]] = {}

    for period, metrics in data.items():
        normalized[period] = {}
        for metric_name, value in metrics.items():
            if pd.isna(value):
                normalized[period][metric_name] = None
            else:
                normalized[period][metric_name] = value.item() if hasattr(value, "item") else value

    return normalized
