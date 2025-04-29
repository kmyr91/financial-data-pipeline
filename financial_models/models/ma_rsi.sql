{{ config(materialized='table') }}

-- 1. Load raw prices
with raw as (
  select
    date,
    ticker,
    adj_close
  from {{ source('financial_data', 'stock_prices') }}
),

-- 2. Calculate price differences
diffs as (
  select
    date,
    ticker,
    adj_close,
    adj_close - lag(adj_close) over (partition by ticker order by date) as price_diff
  from raw
),

-- 3. Separate gains and losses
sides as (
  select
    date,
    ticker,
    adj_close,
    case when price_diff > 0 then price_diff else 0 end as gain,
    case when price_diff < 0 then abs(price_diff) else 0 end as loss
  from diffs
),

-- 4. Compute moving average and average gains/losses
agg as (
  select
    date,
    ticker,
    adj_close,
    avg(adj_close) over (
      partition by ticker
      order by date
      rows between 29 preceding and current row
    ) as ma_30,
    sum(gain) over (
      partition by ticker
      order by date
      rows between 13 preceding and current row
    ) / 14.0 as avg_gain,
    sum(loss) over (
      partition by ticker
      order by date
      rows between 13 preceding and current row
    ) / 14.0 as avg_loss
  from sides
)

-- 5. Final RSI calculation
select
  date,
  ticker,
  adj_close,
  ma_30,
  case
    when avg_loss = 0 then 100
    else 100 - (100 / (1 + avg_gain / avg_loss))
  end as rsi_14
from agg
order by ticker, date;
