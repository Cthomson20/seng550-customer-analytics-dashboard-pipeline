import pandas as pd
import numpy as np
from prophet import Prophet

# discovery and how to use from: https://medium.com/data-science/getting-started-predicting-time-series-data-with-facebook-prophet-c74ad3040525

df = pd.read_csv("dataset/sales_per_year.csv")
df = df[['release_year', 'total_sales']].dropna()
df['ds'] = pd.to_datetime(df['release_year'], format='%Y')
df['y'] = np.log1p(df['total_sales'])

# Fit model
m = Prophet(
    yearly_seasonality=False,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.2,
    n_changepoints=10
)
m.fit(df[['ds', 'y']])

# Forecast (7 years)
future = m.make_future_dataframe(periods=7, freq='Y')
forecast = m.predict(future)

forecast['sales']        = np.expm1(forecast['yhat'])
forecast['sales_lower']  = np.expm1(forecast['yhat_lower'])
forecast['sales_upper']  = np.expm1(forecast['yhat_upper'])
forecast['release_year'] = forecast['ds'].dt.year

# Build unified dataset with ACTUAL + FORECAST
actuals = df.copy()
actuals['sales']       = actuals['total_sales']
actuals['sales_lower'] = None
actuals['sales_upper'] = None
actuals['type']        = 'actual'

forecast_rows = forecast[['release_year','sales','sales_lower','sales_upper']].copy()
forecast_rows['type'] = 'forecast'

# Combine
combined = pd.concat([
    actuals[['release_year','sales','sales_lower','sales_upper','type']],
    forecast_rows[['release_year','sales','sales_lower','sales_upper','type']]
]).sort_values('release_year')

combined.to_csv("sales_forecast_final.csv", index=False)
print("Created file: sales_forecast_final.csv")