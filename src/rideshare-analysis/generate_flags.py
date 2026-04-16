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

            if not rideshare_flag_ever:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in account["transactions"]
                    if item["amount"] < 0
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 1:
                    rideshare_flag_ever1cnt = True

                if count >= 2:
                    rideshare_flag_ever2cnt = True

                if count >= 3:
                    rideshare_flag_ever3cnt = True

                if count >= 4:
                    rideshare_flag_ever = True
                    rideshare_flag_ever4cnt = True

                if count >= 5:
                    rideshare_flag_ever5cnt = True

                if count >= 6:
                    rideshare_flag_ever6cnt = True

                if count >= 7:
                    rideshare_flag_ever7cnt = True

            if not rideshare_flag_30days:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in recent_transactions_30_days
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 1:
                    rideshare_flag_30days1cnt = True

                if count >= 2:
                    rideshare_flag_30days2cnt = True

                if count >= 3:
                    rideshare_flag_30days3cnt = True

                if count >= 4:
                    rideshare_flag_30days = True
                    rideshare_flag_30days4cnt = True

                if count >= 5:
                    rideshare_flag_30days5cnt = True

                if count >= 6:
                    rideshare_flag_30days6cnt = True

                if count >= 7:
                    rideshare_flag_30days7cnt = True

            if not rideshare_flag_60days:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in recent_transactions_60_days
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 1:
                    rideshare_flag_60days1cnt = True

                if count >= 2:
                    rideshare_flag_60days2cnt = True

                if count >= 3:
                    rideshare_flag_60days3cnt = True

                if count >= 4:
                    rideshare_flag_60days = True
                    rideshare_flag_60days4cnt = True

                if count >= 5:
                    rideshare_flag_60days5cnt = True

                if count >= 6:
                    rideshare_flag_60days6cnt = True

                if count >= 7:
                    rideshare_flag_60days7cnt = True

            if not rideshare_flag_90days:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in recent_transactions_90_days
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 1:
                    rideshare_flag_90days1cnt = True

                if count >= 2:
                    rideshare_flag_90days2cnt = True

                if count >= 3:
                    rideshare_flag_90days3cnt = True

                if count >= 4:
                    rideshare_flag_90days = True
                    rideshare_flag_90days4cnt = True

                if count >= 5:
                    rideshare_flag_90days5cnt = True

                if count >= 6:
                    rideshare_flag_90days6cnt = True

                if count >= 7:
                    rideshare_flag_90days7cnt = True

            if not rideshare_flag_6mo:
                count = sum(
                    (item["original_description"].upper().count(keyword))
                    for item in recent_transactions_6mo
                    for keyword in RIDESHARE_LOOKUP_LIST
                )
                if count >= 1:
                    rideshare_flag_6mo1cnt = True

                if count >= 2:
                    rideshare_flag_6mo2cnt = True

                if count >= 3:
                    rideshare_flag_6mo3cnt = True

                if count >= 4:
                    rideshare_flag_6mo = True
                    rideshare_flag_6mo4cnt = True

                if count >= 5:
                    rideshare_flag_6mo5cnt = True

                if count >= 6:
                    rideshare_flag_6mo6cnt = True

                if count >= 7:
                    rideshare_flag_6mo7cnt = True

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
            # "rideshare_flag_ever4cnt": rideshare_flag_ever4cnt,
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
