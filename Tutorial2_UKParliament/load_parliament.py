"""
load_parliament.py
------------------
Loads debates.xlsx via Data2Neo using wrappers.
- Adds a stable UID per row.
- Builds a person->party affiliation map once (best-effort) for pairing.
"""

import uuid
import pandas as pd
from tqdm import tqdm
from data2neo.relational_modules.pandas import PandasDataFrameIterator
from data2neo import Converter, GlobalSharedState
from data2neo.utils import load_file
from parliament_modules import *  # wrappers

# config (try to keep credentials out of code in real use, use env vars or a config file)
# this is just an example, adjust as needed
EXCEL_FILE   = "debates.xlsx"
SHEET_NAME   = "Sheet1"
NEO4J_URI    = "bolt://localhost:7687"
NEO4J_USER   = "neo4j"
NEO4J_PASS   = "password"
SCHEMA_FILE  = "conversion_schema.yaml"

# --- Helpers ---
def split_clean(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    return [v.strip() for v in str(value).split(";") if v and v.strip()]

if __name__ == "__main__":
    # load dataframe
    df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

    # normalize column name for consistency
    df = df.rename(columns={"Corporate Author": "CorporateAuthor"})

    # add a stable uid (deterministic if you prefer: e.g., hash of Ref+Title)
    # here we do a simple random but stable-on-run uid
    if "UID" not in df.columns:
        df["UID"] = [f"DEB_{uuid.uuid4().hex[:10]}" for _ in range(len(df))]

    # ---- Build affiliation map (person -> set(parties)) for better pairing
    affiliation_map = {}
    def add_pairs(member_col, party_col):
        if member_col not in df.columns:
            return
        members_series = df[member_col].fillna("")
        parties_series = df[party_col].fillna("") if party_col in df.columns else ""
        for m_raw, p_raw in zip(members_series, parties_series):
            members = split_clean(m_raw)
            parties = split_clean(p_raw)
            if len(members) == len(parties) and members:
                for m, p in zip(members, parties):
                    key = m.strip().lower()
                    if key not in affiliation_map:
                        affiliation_map[key] = set()
                    if p:
                        affiliation_map[key].add(p.strip())

    add_pairs("Member", "Member Party")
    add_pairs("Lead Member", "Lead Member Party")
    add_pairs("Answering Member", "Answering Member Party")

    GlobalSharedState.affiliations = affiliation_map  # used by wrappers

    # iterator & converter
    iterator = PandasDataFrameIterator(df, "Debates")
    converter = Converter(
        load_file(SCHEMA_FILE),
        iterator,
        NEO4J_URI,
        (NEO4J_USER, NEO4J_PASS),
    )

    print("Starting Data2Neo conversion…")
    converter(progress_bar=tqdm)
    print("Done. Data imported into Neo4j.")