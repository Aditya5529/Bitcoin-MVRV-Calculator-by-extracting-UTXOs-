import pandas as pd
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# === CONFIGURATION ===
REALIZED_CAP_FILE = r"C:\Users\adity\OneDrive\Documents\Bitcoin MVRV\realized_cap.csv"
MVRV_OUTPUT_FILE = "mvrv.csv"

# === STEP 1: Load Realized Cap Data ===
df_realized = pd.read_csv(REALIZED_CAP_FILE)

# Rename if needed (your file uses 'realized_cap')
if 'realized_cap' in df_realized.columns:
    df_realized.rename(columns={'realized_cap': 'realized_cap_usd'}, inplace=True)

df_realized['date'] = pd.to_datetime(df_realized['date']).dt.normalize()

start_date = df_realized['date'].min().date()
end_date = df_realized['date'].max().date()
start_unix = int(datetime.combine(start_date, datetime.min.time()).timestamp())
end_unix = int(datetime.combine(end_date + timedelta(days=1), datetime.min.time()).timestamp())

print(f"📂 Realized Cap loaded from {start_date} to {end_date}")
print("🌐 Fetching BTC price + circulating supply data...")

# === STEP 2: Fetch BTC Price Data from CoinGecko ===
url_price = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
params = {
    "vs_currency": "usd",
    "from": start_unix,
    "to": end_unix
}
price_response = requests.get(url_price, params=params)
price_data = price_response.json()

if 'prices' not in price_data:
    print("❌ Failed to fetch price data from CoinGecko.")
    exit(1)

# Process price data
df_price = pd.DataFrame(price_data['prices'], columns=["timestamp_ms", "btc_price_usd"])
df_price["date"] = pd.to_datetime(df_price["timestamp_ms"], unit="ms").dt.normalize()
df_price = df_price.groupby("date").last().reset_index()[["date", "btc_price_usd"]]

# === STEP 3: Fetch Circulating Supply (Live from CoinGecko) ===
url_info = "https://api.coingecko.com/api/v3/coins/bitcoin"
info_response = requests.get(url_info, params={"localization": "false", "tickers": "false", "market_data": "true"})
btc_info = info_response.json()

circulating_supply = btc_info["market_data"]["circulating_supply"]
print(f"📦 Circulating Supply: {circulating_supply:,.0f} BTC")

# === STEP 4: Merge and Calculate MVRV ===
df_merged = pd.merge(df_realized, df_price, on="date", how="inner")
df_merged["market_cap_usd"] = df_merged["btc_price_usd"] * circulating_supply
df_merged["mvrv_ratio"] = df_merged["market_cap_usd"] / df_merged["realized_cap_usd"]

# === STEP 5: Save to CSV ===
df_merged.to_csv(MVRV_OUTPUT_FILE, index=False)
print(f"✅ MVRV data saved to '{MVRV_OUTPUT_FILE}'")

# === STEP 6: Plot the MVRV Ratio ===
plt.figure(figsize=(12, 5))
plt.plot(df_merged["date"], df_merged["mvrv_ratio"], marker="o", label="MVRV Ratio", color="blue")
plt.axhline(y=1.0, color="red", linestyle="--", label="MVRV = 1 threshold")
plt.title("MVRV Ratio Over Time")
plt.xlabel("Date")
plt.ylabel("MVRV Ratio")
plt.grid(True)
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.show()
