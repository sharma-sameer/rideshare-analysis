import polars as pl
import json
import s3fs
import logging
from itertools import chain
import gc
import re
from .write_to_database import *
from datetime import timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

# BNPL_LOOKUP_LIST = [
#     "AFFIRM",
#     "KLARNA",
#     "AFTERPAY",
#     "SEZZLE",
#     "ZIP",
#     "PAYPAL PAY LATER",
#     "SHOP PAY",
#     "SPLITIT",
#     "SUNBIT",
#     "UPLIFT",
# ]
# GAMBLING_LOOKUP_LIST = [
#     "DRAFTKINGS",
#     "BETFAIR",
#     "BETMGM",
#     "FANDUEL",
#     "CAESARS PALACE ONLINE",
#     "BET365",
#     "CROWN COINS",
#     "STAKE.US",
#     "IGNITION",
#     "BETONLINE",
#     "BETRIVERS",
# ]
RIDESHARE_LOOKUP_LIST = [
    "UBER",
    "LYFT",
    "VIA",
    "REVEL",
    "ALTO",
    "HOPSKIPDRIVE",
    "ZTRIP",
    "WINGZ",
]
# P2P_PLATFORM_LOOKUP_LIST = [
#     "ZELLE",
#     "CASH APP",
#     "PAYPAL",
#     "VENMO",
#     "GOOGLE PAY",
#     "APPLE PAY CASH",
#     "REVOLUT",
#     "CHIME",
#     "WISE",
#     "REMITLY",
# ]
# PAYDAY_LOAN_LOOKUP_LIST = [
#     "EARNIN",
#     "DAVE",
#     "MONEYLION",
#     "BRIGIT",
#     "FLOATME",
#     "KLOVER",
#     "EMPOWER",
#     "CLEO",
#     "ALBERT",
#     "POSSIBLE FINANCE",
# ]
# DSA_LOOKUP_LIST = [
#     "NATIONAL DEBT RELIEF",
#     "FREEDOM DEBT RELIEF",
#     "ACCORDED",
#     "AMERICOR",
#     "BEYOND FINANCE",
#     "CLEARONE ADVANTAGE",
#     "PACIFIC DEBT RELIEF",
#     "STRATEGIC TAX DEBT",
#     "NEW ERA DEBT SOLUTIONS",
#     "GUARDIAN DEBT RELIEF",
# ]

# pattern_bnpl = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in BNPL_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_gambling = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in GAMBLING_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
pattern_rideshare = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in RIDESHARE_LOOKUP_LIST) + r")\b",
    re.IGNORECASE,
)
# pattern_p2p = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in P2P_PLATFORM_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_payday = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in PAYDAY_LOAN_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )
# pattern_dsa = re.compile(
#     r"\b(" + "|".join(re.escape(k) for k in DSA_LOOKUP_LIST) + r")\b",
#     re.IGNORECASE,
# )


def process_chunk(chunk):
    chunk_id, files = chunk

    flags = list()

    for f in files:
        df = pl.read_parquet(f)
        try:
            df = df.with_columns(
                pl.Series(
                    "DATA", [json.loads(s) for s in df["DATA"]], strict=False
                )
            )
        except Exception as e:
            logger.info(f"Data is: {df.head(1)}")
            continue

        flags.append(_get_flags(df))

    logger.info("Converting this 2D list to 1D list.")
    flags = list(chain.from_iterable(flags))
    if save_to_snowflake(pl.DataFrame(flags)):
        return True
    return False


def _get_flags(df):

    rows = df.partition_by("APPL_KEY", include_key=True)

    all_flags = []
    for this_iteration in rows:
        accounts = this_iteration["DATA"][0]["report"]["items"][0]
        rideshare_flag_ever = rideshare_flag_30days = rideshare_flag_60days = (
            rideshare_flag_90days
        ) = rideshare_flag_6mo = False
        rideshare_flag_ever1cnt = rideshare_flag_ever2cnt = (
            rideshare_flag_ever3cnt
        ) = rideshare_flag_ever4cnt = rideshare_flag_ever5cnt = (
            rideshare_flag_ever6cnt
        ) = rideshare_flag_ever7cnt = False
        rideshare_flag_30days1cnt = rideshare_flag_30days2cnt = (
            rideshare_flag_30days3cnt
        ) = rideshare_flag_30days4cnt = rideshare_flag_30days5cnt = (
            rideshare_flag_30days6cnt
        ) = rideshare_flag_30days7cnt = False
        rideshare_flag_60days1cnt = rideshare_flag_60days2cnt = (
            rideshare_flag_60days3cnt
        ) = rideshare_flag_60days4cnt = rideshare_flag_60days5cnt = (
            rideshare_flag_60days6cnt
        ) = rideshare_flag_60days7cnt = False
        rideshare_flag_90days1cnt = rideshare_flag_90days2cnt = (
            rideshare_flag_90days3cnt
        ) = rideshare_flag_90days4cnt = rideshare_flag_90days5cnt = (
            rideshare_flag_90days6cnt
        ) = rideshare_flag_90days7cnt = False
        rideshare_flag_6mo1cnt = rideshare_flag_6mo2cnt = (
            rideshare_flag_6mo3cnt
        ) = rideshare_flag_6mo4cnt = rideshare_flag_6mo5cnt = (
            rideshare_flag_6mo6cnt
        ) = rideshare_flag_6mo7cnt = False
        for account in accounts["accounts"]:
            balance_updated = this_iteration["APPL_ENTRY_DT"][0]
            cutoff_30_days = balance_updated - timedelta(days=30)
            cutoff_60_days = balance_updated - timedelta(days=60)
            cutoff_90_days = balance_updated - timedelta(days=90)
            cutoff_6_mo = balance_updated - relativedelta(months=6)

            # Pre-calculate the subset to scan for the window flags.
            recent_transactions_30_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_30_days
            ]

            recent_transactions_60_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_60_days
            ]

            recent_transactions_90_days = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date()
                >= cutoff_90_days
            ]

            recent_transactions_6mo = [
                item
                for item in account["transactions"]
                if dt.strptime(item["date"], "%Y-%m-%d").date() >= cutoff_6_mo
            ]
            # if not bnpl_flag:
            #     bnpl_flag = any(
            #         keyword in item["original_description"].upper()
            #         for item in account["transactions"]
            #         for keyword in BNPL_LOOKUP_LIST
            #     )
            # if not gambling_flag:
            #     gambling_flag = any(
            #         keyword in item["original_description"].upper()
            #         for item in account["transactions"]
            #         for keyword in GAMBLING_LOOKUP_LIST
            #     )

            rideshare_flag_ever = rideshare_flag_ever or _check_credit_pattern(
                pl.DataFrame(account["transactions"]), RIDESHARE_LOOKUP_LIST, 4
            )
            rideshare_flag_ever1cnt = (
                rideshare_flag_ever1cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    1,
                )
            )
            rideshare_flag_ever2cnt = (
                rideshare_flag_ever2cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    2,
                )
            )
            rideshare_flag_ever3cnt = (
                rideshare_flag_ever3cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    3,
                )
            )
            rideshare_flag_ever4cnt = (
                rideshare_flag_ever4cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_ever5cnt = (
                rideshare_flag_ever5cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    5,
                )
            )
            rideshare_flag_ever6cnt = (
                rideshare_flag_ever6cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    6,
                )
            )
            rideshare_flag_ever7cnt = (
                rideshare_flag_ever7cnt
                or _check_credit_pattern(
                    pl.DataFrame(account["transactions"]),
                    RIDESHARE_LOOKUP_LIST,
                    7,
                )
            )

            rideshare_flag_30days = (
                rideshare_flag_30days
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_30days1cnt = (
                rideshare_flag_30days1cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    1,
                )
            )
            rideshare_flag_30days2cnt = (
                rideshare_flag_30days2cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    2,
                )
            )
            rideshare_flag_30days3cnt = (
                rideshare_flag_30days3cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    3,
                )
            )
            rideshare_flag_30days4cnt = (
                rideshare_flag_30days4cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_30days5cnt = (
                rideshare_flag_30days5cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    5,
                )
            )
            rideshare_flag_30days6cnt = (
                rideshare_flag_30days6cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    6,
                )
            )
            rideshare_flag_30days7cnt = (
                rideshare_flag_30days7cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_30_days),
                    RIDESHARE_LOOKUP_LIST,
                    7,
                )
            )

            rideshare_flag_60days = (
                rideshare_flag_60days
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_60days1cnt = (
                rideshare_flag_60days1cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    1,
                )
            )
            rideshare_flag_60days2cnt = (
                rideshare_flag_60days2cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    2,
                )
            )
            rideshare_flag_60days3cnt = (
                rideshare_flag_60days3cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    3,
                )
            )
            rideshare_flag_60days4cnt = (
                rideshare_flag_60days4cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_60days5cnt = (
                rideshare_flag_60days5cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    5,
                )
            )
            rideshare_flag_60days6cnt = (
                rideshare_flag_60days6cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    6,
                )
            )
            rideshare_flag_60days7cnt = (
                rideshare_flag_60days7cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_60_days),
                    RIDESHARE_LOOKUP_LIST,
                    7,
                )
            )

            rideshare_flag_90days = (
                rideshare_flag_90days
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_90days1cnt = (
                rideshare_flag_90days1cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    1,
                )
            )
            rideshare_flag_90days2cnt = (
                rideshare_flag_90days2cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    2,
                )
            )
            rideshare_flag_90days3cnt = (
                rideshare_flag_90days3cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    3,
                )
            )
            rideshare_flag_90days4cnt = (
                rideshare_flag_90days4cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_90days5cnt = (
                rideshare_flag_90days5cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    5,
                )
            )
            rideshare_flag_90days6cnt = (
                rideshare_flag_90days6cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    6,
                )
            )
            rideshare_flag_90days7cnt = (
                rideshare_flag_90days7cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_90_days),
                    RIDESHARE_LOOKUP_LIST,
                    7,
                )
            )

            rideshare_flag_6mo = rideshare_flag_6mo or _check_credit_pattern(
                pl.DataFrame(recent_transactions_6mo), RIDESHARE_LOOKUP_LIST, 4
            )
            rideshare_flag_6mo1cnt = (
                rideshare_flag_6mo1cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    1,
                )
            )
            rideshare_flag_6mo2cnt = (
                rideshare_flag_6mo2cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    2,
                )
            )
            rideshare_flag_6mo3cnt = (
                rideshare_flag_6mo3cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    3,
                )
            )
            rideshare_flag_6mo4cnt = (
                rideshare_flag_6mo4cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    4,
                )
            )
            rideshare_flag_6mo5cnt = (
                rideshare_flag_6mo5cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    5,
                )
            )
            rideshare_flag_6mo6cnt = (
                rideshare_flag_6mo6cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    6,
                )
            )
            rideshare_flag_6mo7cnt = (
                rideshare_flag_6mo7cnt
                or _check_credit_pattern(
                    pl.DataFrame(recent_transactions_6mo),
                    RIDESHARE_LOOKUP_LIST,
                    7,
                )
            )

            # if not p2p_flag:
            #     p2p_flag = any(
            #         keyword in item["original_description"].upper()
            #         for item in account["transactions"]
            #         for keyword in P2P_PLATFORM_LOOKUP_LIST
            #     )
            # if not payday_flag:
            #     payday_flag = any(
            #         keyword in item["original_description"].upper()
            #         for item in account["transactions"]
            #         for keyword in PAYDAY_LOAN_LOOKUP_LIST
            #     )
            # if not dsa_flag:
            #     dsa_flag = any(
            #         keyword in item["original_description"].upper()
            #         for item in account["transactions"]
            #         for keyword in DSA_LOOKUP_LIST
            #     )

        flags_dict = {
            "appl_key": this_iteration["APPL_KEY"][0],
            "acap_key": this_iteration["ACAP_REFR_ID"][0],
            # "has_bnpl_ever": bnpl_flag,
            # "has_gambling_ever": gambling_flag,
            "has_rideshare_ever": rideshare_flag_ever,
            "rideshare_flag_ever1cnt": rideshare_flag_ever1cnt,
            "rideshare_flag_ever2cnt": rideshare_flag_ever2cnt,
            "rideshare_flag_ever3cnt": rideshare_flag_ever3cnt,
            "rideshare_flag_ever4cnt": rideshare_flag_ever4cnt,
            "rideshare_flag_ever5cnt": rideshare_flag_ever5cnt,
            "rideshare_flag_ever6cnt": rideshare_flag_ever6cnt,
            "rideshare_flag_ever7cnt": rideshare_flag_ever7cnt,
            "has_rideshare_30days": rideshare_flag_30days,
            "rideshare_flag_30days1cnt": rideshare_flag_30days1cnt,
            "rideshare_flag_30days2cnt": rideshare_flag_30days2cnt,
            "rideshare_flag_30days3cnt": rideshare_flag_30days3cnt,
            "rideshare_flag_30days4cnt": rideshare_flag_30days4cnt,
            "rideshare_flag_30days5cnt": rideshare_flag_30days5cnt,
            "rideshare_flag_30days6cnt": rideshare_flag_30days6cnt,
            "rideshare_flag_30days7cnt": rideshare_flag_30days7cnt,
            "has_rideshare_60days": rideshare_flag_60days,
            "rideshare_flag_60days1cnt": rideshare_flag_60days1cnt,
            "rideshare_flag_60days2cnt": rideshare_flag_60days2cnt,
            "rideshare_flag_60days3cnt": rideshare_flag_60days3cnt,
            "rideshare_flag_60days4cnt": rideshare_flag_60days4cnt,
            "rideshare_flag_60days5cnt": rideshare_flag_60days5cnt,
            "rideshare_flag_60days6cnt": rideshare_flag_60days6cnt,
            "rideshare_flag_60days7cnt": rideshare_flag_60days7cnt,
            "has_rideshare_90days": rideshare_flag_90days,
            "rideshare_flag_90days1cnt": rideshare_flag_90days1cnt,
            "rideshare_flag_90days2cnt": rideshare_flag_90days2cnt,
            "rideshare_flag_90days3cnt": rideshare_flag_90days3cnt,
            "rideshare_flag_90days4cnt": rideshare_flag_90days4cnt,
            "rideshare_flag_90days5cnt": rideshare_flag_90days5cnt,
            "rideshare_flag_90days6cnt": rideshare_flag_90days6cnt,
            "rideshare_flag_90days7cnt": rideshare_flag_90days7cnt,
            "has_rideshare_6mo": rideshare_flag_6mo,
            "rideshare_flag_6mo1cnt": rideshare_flag_6mo1cnt,
            "rideshare_flag_6mo2cnt": rideshare_flag_6mo2cnt,
            "rideshare_flag_6mo3cnt": rideshare_flag_6mo3cnt,
            "rideshare_flag_6mo4cnt": rideshare_flag_6mo4cnt,
            "rideshare_flag_6mo5cnt": rideshare_flag_6mo5cnt,
            "rideshare_flag_6mo6cnt": rideshare_flag_6mo6cnt,
            "rideshare_flag_6mo7cnt": rideshare_flag_6mo7cnt,
            # "has_p2p_ever": p2p_flag,
            # "has_payday_ever": payday_flag,
            # "has_dsa_ever": dsa_flag,
            "appl_entry_dt": this_iteration["APPL_ENTRY_DT"][0],
        }
        all_flags.append(flags_dict)

    return all_flags


def _check_credit_pattern(df, keywords, x_threshold, window="1mo"):
    # 1. Ensure date is sorted (required for dynamic grouping)
    df = df.sort("date")

    # 2. Build case-insensitive regex pattern from keywords list
    keyword_regex = "(?i)" + "|".join(keywords)

    # 3. Filter for:
    #    - Credits (amount < 0)
    #    - Descriptions matching keywords
    matched_credits = df.filter(
        (pl.col("amount") < 0)
        & (pl.col("original_description").str.contains(keyword_regex))
    )

    # 4. Use group_by_dynamic to check a 1-month window starting every 1 day
    # This creates a "moving" window effect
    results = (
        matched_credits.group_by_dynamic("date", every="1d", period=window).agg(
            [
                pl.len().alias("count"),
                pl.col("id").alias("transaction_ids"),
                pl.col("amount").sum().alias("total_window_value"),
            ]
        )
        # 5. Only return windows where count >= x
        .filter(pl.col("count") >= x_threshold)
    )

    return results
