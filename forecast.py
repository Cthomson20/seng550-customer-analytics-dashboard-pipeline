import pandas as pd
from prophet import Prophet

# discovery and how to use from: https://medium.com/data-science/getting-started-predicting-time-series-data-with-facebook-prophet-c74ad3040525


# read
df = pd.read_csv("dataset/sales_per_year.csv")

# drop null
df = df[['release_year', 'total_sales']].dropna()

# build dataframe
df['ds'] = pd.to_datetime(df['release_year'], format='%Y')
df['y'] = df['total_sales']

df_prophet = df[['ds', 'y']]

# call prophet
m = Prophet()
m.fit(df_prophet)

# Create forecast (next 10 years)
future = m.make_future_dataframe(periods=10, freq='Y')

# forecast
forecast = m.predict(future)

out = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
out['release_year'] = out['ds'].dt.year

# Merge actual + forecast
merged = pd.merge(
    out,
    df[['release_year', 'total_sales']],
    on='release_year',
    how='left'
)

# save
merged.to_csv("sales_by_year_forecast.csv", index=False)
print("Saved forecast to sales_by_year_forecast.csv")
