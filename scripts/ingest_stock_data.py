#!/usr/bin/env python3
"""
ingest_stock_data.py

Fetches daily stock price data for given tickers and ingests it into DuckDB.
Usage:
    python scripts/ingest_stock_data.py \
      --tickers SPY AAPL \
      --start_date 2020-01-01 \
      --end_date 2025-04-28
"""

import argparse
import os

import yfinance as yf
import duckdb
import pandas as pd


def fetch_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download historical OHLCV data and return a DataFrame with lowercase columns.
    """
    df = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        auto_adjust=False,
        progress=False,
        actions=False,
    )
    df.reset_index(inplace=True)
    # Ensure 'adj_close' present
    if 'Adj Close' in df.columns:
        df.rename(columns={'Adj Close': 'adj_close'}, inplace=True)
    else:
        df['adj_close'] = df['Close']
    # Add ticker column and normalize names
    df['ticker'] = ticker
    df.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    }, inplace=True)
    # Select and order columns explicitly
    df = df[['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'ticker']]
    return df


def ingest_to_duckdb(df: pd.DataFrame, db_path: str, table_name: str = 'stock_prices'):
    """
    Create or append stock price data into DuckDB.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(database=db_path, read_only=False)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume BIGINT,
            ticker VARCHAR
        )
    """)
    con.register('df', df)
    # Use SELECT * since df columns match table schema
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest stock price data into DuckDB")
    parser.add_argument('--tickers', nargs='+', required=True,
                        help="Ticker symbols (e.g., SPY AAPL)")
    parser.add_argument('--start_date', type=str, default='2020-01-01',
                        help="Start date YYYY-MM-DD")
    parser.add_argument('--end_date', type=str, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument('--db_path', type=str, default='data/financial.duckdb',
                        help="DuckDB file path")
    args = parser.parse_args()

    for ticker in args.tickers:
        print(f"Fetching data for {ticker}...")
        df = fetch_data(ticker, args.start_date, args.end_date)
        print(f"Ingesting {len(df)} rows for {ticker}...")
        ingest_to_duckdb(df, args.db_path)

    print("Done.")


if __name__ == '__main__':
    main()
