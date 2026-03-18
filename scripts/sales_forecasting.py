# scripts/sales_forecasting.py
# Prophet time-series forecasting on monthly revenue

import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from dotenv import load_dotenv
import os

load_dotenv()

# ── 1. Connect and pull monthly revenue ──────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="ECOMMERCE_WH",
    database="ECOMMERCE_DB",
    schema="MARTS",
    role="ACCOUNTADMIN"
)

print("Pulling monthly revenue...")
revenue_df = pd.read_sql("""
    SELECT
        ORDER_MONTH,
        TOTAL_REVENUE
    FROM MARTS.MART_MONTHLY_REVENUE
    ORDER BY ORDER_MONTH
""", conn)

print(f"  Loaded {len(revenue_df)} months of revenue data")
conn.close()

# ── 2. Prepare for Prophet ────────────────────────────────────────────────────
# Prophet requires columns named 'ds' and 'y'
revenue_df = revenue_df.rename(columns={
    'ORDER_MONTH': 'ds',
    'TOTAL_REVENUE': 'y'
})
revenue_df['ds'] = pd.to_datetime(revenue_df['ds'])

print("\n── Historical Revenue Summary ──────────────────────")
print(f"  Date range: {revenue_df['ds'].min().date()} → {revenue_df['ds'].max().date()}")
print(f"  Avg monthly revenue: ${revenue_df['y'].mean():,.2f}")
print(f"  Peak monthly revenue: ${revenue_df['y'].max():,.2f}")
print(f"  Min monthly revenue:  ${revenue_df['y'].min():,.2f}")

# ── 3. Train Prophet model ────────────────────────────────────────────────────
print("\nTraining Prophet model...")
model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.05
)
model.fit(revenue_df)

# ── 4. Forecast next 3 months ─────────────────────────────────────────────────
print("Forecasting next 3 months...")
future = model.make_future_dataframe(periods=3, freq='MS')
forecast = model.predict(future)

print("\n── 3-Month Revenue Forecast ────────────────────────")
forecast_tail = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(3)
for _, row in forecast_tail.iterrows():
    print(f"  {row['ds'].strftime('%b %Y')}:  "
          f"${row['yhat']:>10,.2f}  "
          f"(range: ${row['yhat_lower']:,.2f} – ${row['yhat_upper']:,.2f})")

# ── 5. Plot forecast ──────────────────────────────────────────────────────────
print("\nGenerating forecast plot...")
os.makedirs('visuals', exist_ok=True)

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(revenue_df['ds'], revenue_df['y'], 'b-o', label='Actual Revenue', linewidth=2)
ax.plot(forecast['ds'], forecast['yhat'], 'r--', label='Forecast', linewidth=2)
ax.fill_between(forecast['ds'],
                forecast['yhat_lower'],
                forecast['yhat_upper'],
                alpha=0.2, color='red', label='Confidence Interval')
ax.axvline(x=revenue_df['ds'].max(), color='gray', linestyle=':', label='Forecast Start')
ax.set_title('Monthly Revenue Forecast — Next 3 Months', fontsize=14)
ax.set_xlabel('Date')
ax.set_ylabel('Revenue ($)')
ax.legend()
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,.0f}'))
plt.tight_layout()
plt.savefig('visuals/revenue_forecast.png', dpi=150)
plt.close()
print("  Saved visuals/revenue_forecast.png")

# ── 6. Save results ───────────────────────────────────────────────────────────
forecast.to_csv('visuals/revenue_forecast.csv', index=False)
print("  Saved visuals/revenue_forecast.csv")

print("\n✅ Sales forecasting complete!")