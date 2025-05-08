import pandas as pd
import requests
from datetime import datetime
import os

# Load UTXOs
utxos = pd.read_csv(r"C:\Users\adity\OneDrive\Documents\Bitcoin MVRV\utxos.csv")
utxos['timestamp'] = pd.to_datetime(utxos['block_time'], unit='s')

utxos = utxos.sort_values('timestamp')

start_ts = utxos['timestamp'].min()
end_ts = utxos['timestamp'].max()
start_unix = int(start_ts.timestamp())
end_unix = int(end_ts.timestamp())

print(f"Fetching BTC price data from {start_ts} to {end_ts}...")

#  Fetch BTC Prices 
url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
params = {
    'vs_currency': 'usd',
    'from': start_unix,
    'to': end_unix
}
response = requests.get(url, params=params)
response.raise_for_status()
btc_prices_raw = response.json()['prices']

btc_prices = pd.DataFrame(btc_prices_raw, columns=['timestamp_ms', 'price'])
btc_prices['timestamp'] = pd.to_datetime(btc_prices['timestamp_ms'], unit='ms')
btc_prices.drop(columns=['timestamp_ms'], inplace=True)
btc_prices = btc_prices.sort_values('timestamp')

merged = pd.merge_asof(
    utxos.sort_values('timestamp'),
    btc_prices,
    on='timestamp',
    direction='backward'
)

print(" Columns in merged:", merged.columns.tolist())

# Calculate Realized Cap 
merged['realized_cap'] = merged['amount_btc'] * merged['price']

#  Group by Day 
merged['date'] = merged['timestamp'].dt.date
daily_realized_cap = merged.groupby('date')['realized_cap'].sum().reset_index()

#  Save Result 
output_file = "realized_cap.csv"
daily_realized_cap.to_csv(output_file, index=False)

print(f"✅ Realized cap saved to: {output_file}")
import matplotlib.pyplot as plt

df = pd.read_csv("realized_cap.csv")
df['date'] = pd.to_datetime(df['date'])

plt.figure(figsize=(12, 5))
plt.plot(df['date'], df['realized_cap'], marker='o', label="Realized Cap (USD)")
plt.title("Realized Cap Trend")
plt.xlabel("Date")
plt.ylabel("Realized Cap ($)")
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.legend()
plt.show()

