#!/usr/bin/env python3
"""
ingest_stock_data.py

Fetches daily stock price data for given tickers and ingests it into DuckDB.
Usage:
    python scripts/ingest_stock_data.py --tickers SPY AAPL --start_date 2020-01-01 --end_date 2025-04-28
"""

import argparse
import os

import yfinance as yf
import duckdb
import pandas as pd


def fetch_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download historical OHLCV data for a single ticker and return as a DataFrame.
    """
    df = yf.download(ticker, start=start_date, end=end_date)
    df.reset_index(inplace=True)
    df.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
    df['Ticker'] = ticker
    return df


def ingest_to_duckdb(df: pd.DataFrame, db_path: str, table_name: str = 'stock_prices'):
    """
    Create or append stock price data into a DuckDB table.
    """
    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    con = duckdb.connect(database=db_path, read_only=False)
    # Create table if not exists
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date DATE,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Adj_Close DOUBLE,
            Volume BIGINT,
            Ticker VARCHAR
        )
    """)
    # Register DataFrame and insert
    con.register('df', df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest stock price data into DuckDB")
    parser.add_argument('--tickers', nargs='+', required=True, help="List of ticker symbols (e.g., SPY AAPL)")
    parser.add_argument('--start_date', type=str, default='2020-01-01', help="Start date YYYY-MM-DD")
    parser.add_argument('--end_date', type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument('--db_path', type=str, default='data/financial.duckdb', help="Path to DuckDB database file")
    args = parser.parse_args()

    for ticker in args.tickers:
        print(f"Fetching data for {ticker}...")
        df = fetch_data(ticker, args.start_date, args.end_date)
        print(f"Ingesting {len(df)} rows for {ticker} into DuckDB...")
        ingest_to_duckdb(df, args.db_path)

    print("Done.")


if __name__ == '__main__':
    main()
