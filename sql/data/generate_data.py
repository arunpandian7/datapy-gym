#!/usr/bin/env python3
"""Generate synthetic fintech/payments CSV data for the SQL practice track."""
import pandas as pd
import numpy as np
from pathlib import Path

OUT_DIR = Path(__file__).parent
RNG = np.random.default_rng(42)

# ── users ──────────────────────────────────────────────────────────────────────

FIRST = ["Alice","Bob","Carlos","Diana","Ethan","Fatima","George","Hannah","Ivan",
         "Jaya","Kevin","Layla","Marcus","Nina","Omar","Priya","Quinn","Rosa",
         "Samuel","Tara","Umar","Vera","William","Xiu","Yusuf","Zoe","Aiko",
         "Bram","Chloe","Dev"]
LAST  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
         "Wilson","Martinez","Anderson","Taylor","Thomas","Hernandez","Moore",
         "Martin","Jackson","Thompson","White","Lopez","Lee","Gonzalez","Harris",
         "Clark","Lewis","Robinson","Walker","Perez","Hall","Young"]

N_USERS = 500
user_ids   = np.arange(1, N_USERS + 1)
names      = [f"{RNG.choice(FIRST)} {RNG.choice(LAST)}" for _ in range(N_USERS)]
emails     = [f"user{i}@example.com" for i in user_ids]
countries  = RNG.choice(["US","UK","IN","SG","AU"],
                        size=N_USERS, p=[0.40, 0.20, 0.20, 0.10, 0.10])
tiers      = RNG.choice(["basic","premium","business"],
                        size=N_USERS, p=[0.50, 0.30, 0.20])
kyc        = RNG.choice([True, False], size=N_USERS, p=[0.90, 0.10])
created    = pd.to_datetime("2020-01-01") + pd.to_timedelta(
    RNG.integers(0, (pd.to_datetime("2023-06-30") - pd.to_datetime("2020-01-01")).days, N_USERS), "D"
)

users = pd.DataFrame({
    "user_id":      user_ids,
    "name":         names,
    "email":        emails,
    "country":      countries,
    "tier":         tiers,
    "kyc_verified": kyc,
    "created_date": created.date,
})

# ── merchants ─────────────────────────────────────────────────────────────────

MCC_CATEGORIES = ["groceries","restaurants","travel","entertainment",
                  "utilities","retail","healthcare","fuel"]
BRANDS = {
    "groceries":    ["FreshMart","GreenLeaf","DailyBasket"],
    "restaurants":  ["QuickBite","TastyTable","UrbanEats"],
    "travel":       ["SkyWay","JetSet","RoamEasy"],
    "entertainment":["CinePlex","PlayZone","LiveBeat"],
    "utilities":    ["PowerGrid","AquaFlow","NetConnect"],
    "retail":       ["TrendStore","MegaShop","StyleHub"],
    "healthcare":   ["MediCare","WellnessPlus","PharmaDirect"],
    "fuel":         ["FuelStop","DriveGo","GasPro"],
}
CITIES = ["New York","London","Mumbai","Singapore","Sydney",
          "Chicago","Manchester","Delhi","Austin","Los Angeles"]
MERCHANT_COUNTRIES = ["US","UK","IN","SG","AU"]

N_MERCHANTS = 200
merch_ids   = np.arange(1, N_MERCHANTS + 1)
mcc_cats    = np.repeat(MCC_CATEGORIES, N_MERCHANTS // len(MCC_CATEGORIES))
merch_names = [f"{RNG.choice(BRANDS[c])} {c.title()} {i%25+1}" for i, c in enumerate(mcc_cats)]
merch_city  = RNG.choice(CITIES, size=N_MERCHANTS)
merch_country = RNG.choice(MERCHANT_COUNTRIES, size=N_MERCHANTS, p=[0.50,0.20,0.15,0.10,0.05])

merchants = pd.DataFrame({
    "merchant_id":   merch_ids,
    "name":          merch_names,
    "mcc_category":  mcc_cats,
    "city":          merch_city,
    "country":       merch_country,
})

# ── accounts ──────────────────────────────────────────────────────────────────

# ~20% of users get a second account
# Last 25 users (476–500) intentionally have no accounts, so anti-join problems return real results
N_USERS_WITH_ACCOUNTS = 475
users_with_accounts = user_ids[:N_USERS_WITH_ACCOUNTS]

# ~20% of active users get a second account
second_acct_users = RNG.choice(users_with_accounts, size=100, replace=False)
acct_users = np.concatenate([users_with_accounts, second_acct_users])
N_ACCOUNTS = len(acct_users)
acct_ids   = np.arange(1, N_ACCOUNTS + 1)
acct_types = RNG.choice(["checking","savings","credit"],
                        size=N_ACCOUNTS, p=[0.50, 0.30, 0.20])
acct_status = RNG.choice(["active","closed","suspended"],
                          size=N_ACCOUNTS, p=[0.80, 0.15, 0.05])

# opened_date >= user's created_date
user_created = dict(zip(users["user_id"], users["created_date"]))
opened_dates = []
for uid in acct_users:
    base = pd.to_datetime(user_created[uid])
    days_after = int(RNG.integers(0, 365))
    opened_dates.append((base + pd.Timedelta(days=days_after)).date())

balances = np.round(RNG.uniform(-5_000, 50_000, N_ACCOUNTS), 2)

accounts = pd.DataFrame({
    "account_id":  acct_ids,
    "user_id":     acct_users,
    "account_type": acct_types,
    "opened_date": opened_dates,
    "balance":     balances,
    "status":      acct_status,
})

# ── transactions ──────────────────────────────────────────────────────────────

N_TXN = 20_000
TXN_START = pd.to_datetime("2022-01-01")
TXN_END   = pd.to_datetime("2024-12-31")

# skew: top 20 accounts get ~30% of transactions
top_accounts   = RNG.choice(acct_ids, size=20, replace=False)
normal_accounts = np.setdiff1d(acct_ids, top_accounts)

n_skewed = int(N_TXN * 0.30)
n_normal = N_TXN - n_skewed

txn_accounts = np.concatenate([
    RNG.choice(top_accounts,   size=n_skewed),
    RNG.choice(normal_accounts, size=n_normal),
])
RNG.shuffle(txn_accounts)

# skew: top 20 merchants get ~25% of transactions
top_merch   = RNG.choice(merch_ids, size=20, replace=False)
normal_merch = np.setdiff1d(merch_ids, top_merch)

n_merch_skewed = int(N_TXN * 0.25)
n_merch_normal = N_TXN - n_merch_skewed

txn_merchants = np.concatenate([
    RNG.choice(top_merch,    size=n_merch_skewed),
    RNG.choice(normal_merch, size=n_merch_normal),
])
RNG.shuffle(txn_merchants)

# amounts: log-normal distribution for realistic transaction sizes
raw_amounts = np.clip(np.round(RNG.lognormal(mean=4.0, sigma=1.5, size=N_TXN), 2), 1.00, 9999.99)

txn_dates = TXN_START + pd.to_timedelta(
    RNG.integers(0, (TXN_END - TXN_START).days + 1, N_TXN), "D"
)
txn_types  = RNG.choice(["debit","credit"], size=N_TXN, p=[0.75, 0.25])
txn_status = RNG.choice(["completed","failed","pending","reversed"],
                        size=N_TXN, p=[0.80, 0.10, 0.07, 0.03])

transactions = pd.DataFrame({
    "txn_id":      np.arange(1, N_TXN + 1),
    "account_id":  txn_accounts,
    "merchant_id": txn_merchants,
    "amount":      raw_amounts,
    "txn_type":    txn_types,
    "txn_date":    txn_dates.date,
    "status":      txn_status,
})

# ── write CSVs ────────────────────────────────────────────────────────────────

for name, df in [("users", users), ("merchants", merchants),
                 ("accounts", accounts), ("transactions", transactions)]:
    path = OUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  {name:<14} {len(df):>7,} rows  →  {path.name}")

print("\nDone. Regenerate any time with: uv run python sql/data/generate_data.py")
