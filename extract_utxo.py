import subprocess
import json
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# --- CONFIGURATION ---
START_BLOCK = 895500
END_BLOCK = 895795
BLOCK_STEP = 10           # Sample every 10th block
THREADS = 16              # Adjust based on CPU
OUTPUT_FILE = "utxos.csv"

# --- THREADING LOCK FOR CSV ---
write_lock = threading.Lock()

# --- BITCOIN CLI HELPERS ---

def run_cli_raw(*args):
    command = ["bitcoin-cli"] + list(args)
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if result.returncode != 0:
            print(f"❌ Error: {' '.join(command)}\n{result.stderr.strip()}")
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        print(f"⏳ Timeout: {' '.join(command)}")
        return None

def run_cli_json(*args):
    output = run_cli_raw(*args)
    if output is None:
        return None
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        print(f"❌ JSON error: {e}\nOutput: {output}")
        return None

def check_bitcoind_running():
    if run_cli_raw("getblockcount") is None:
        print("❌ bitcoind is not running. Start your node.")
        exit(1)

# --- BLOCK PROCESSING FUNCTION ---

def process_block(height):
    utxos = []
    print(f"🔄 Processing block {height}...")
    block_hash = run_cli_raw("getblockhash", str(height))
    if not block_hash:
        print(f"⚠️ Skipping block {height}: no block hash.")
        return []

    block = run_cli_json("getblock", block_hash, "2")
    if not block:
        print(f"⚠️ Skipping block {height}: could not fetch block data.")
        return []

    block_time = block.get("time")
    for tx in block.get("tx", []):
        txid = tx["txid"]
        for vout in tx["vout"]:
            vout_index = vout["n"]
            amount = vout["value"]
            txout = run_cli_json("gettxout", txid, str(vout_index))
            if txout:
                utxos.append([height, txid, vout_index, amount, block_time])
    return utxos

# --- MAIN FUNCTION ---

def extract_utxos_parallel():
    start_time = time.time()
    print(f"🚀 Starting UTXO extraction with {THREADS} threads from block {START_BLOCK} to {END_BLOCK}...")

    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["block_height", "txid", "vout", "amount_btc", "block_time"])

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {
                executor.submit(process_block, height): height
                for height in range(START_BLOCK, END_BLOCK + 1, BLOCK_STEP)
            }

            for future in as_completed(futures):
                block_utxos = future.result()
                if block_utxos:
                    with write_lock:
                        writer.writerows(block_utxos)

    print(f"🎉 UTXO extraction complete. Saved to {OUTPUT_FILE}")
    print(f"⏱️ Total time: {(time.time() - start_time)/60:.2f} minutes")

# --- ENTRY POINT ---

if __name__ == "__main__":
    check_bitcoind_running()
    extract_utxos_parallel()
