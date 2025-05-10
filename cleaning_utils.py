import pandas as pd
import os
import re
import json
import logging
import argparse

# Constants
PRICE_FIELDS = [
    "negotiated price", "negotiated percentage",
    "gross charge", "discounted cash price", "min price", "max price", "estimated amount"
]

TEXT_FIELDS = [
    "insurance payer name", "insurance plan name", "description", "setting", 
    "negotiated algorithm", "negotiated methodology"
]

PLACEHOLDER_VALUE = "999999999"

def apply_conditional_rules(df):
    violations = {}
    mask1 = df[PRICE_FIELDS[:3]].notna().any(axis=1) & (~df[["insurance payer name", "insurance plan name", "negotiated methodology"]].notna().all(axis=1))
    violations['rule_1'] = mask1
    mask2 = df[PRICE_FIELDS].notna().any(axis=1) & (~df[["code", "code type"]].notna().all(axis=1))
    violations['rule_2'] = mask2
    mask3 = (df["code"].notna() & df["code type"].isna()) | (df["code type"].notna() & df["code"].isna())
    violations['rule_3'] = mask3
    mask4 = (df["negotiated methodology"].str.lower() == "other") & df["additional notes"].isna()
    violations['rule_4'] = mask4
    required_price_fields = [
    "gross charge", "discounted cash price",
    "negotiated price", "negotiated percentage", "negotiated algorithm"
    ]
    mask5 = df["description"].notna() & df[required_price_fields].isna().all(axis=1)
    violations['rule_5'] = mask5
    mask6 = df["negotiated price"].notna() & (~df[["min price", "max price"]].notna().all(axis=1))
    violations['rule_6'] = mask6
    has_percent_or_algo = df[["negotiated percentage", "negotiated algorithm"]].notna().any(axis=1)
    mask7 = (
        df["negotiated price"].isna() &
        has_percent_or_algo &
        df["estimated amount"].isna()
    )
    violations['rule_7'] = mask7
    mask8 = df["code type"].str.upper() == "NDC"
    mask8 &= (~df[["drug unit", "drug type"]].notna().all(axis=1))
    violations['rule_8'] = mask8
    mask9 = df["modifiers"].notna() & df["description"].isna()
    mask9 &= df[["negotiated price", "negotiated percentage", "negotiated algorithm", "additional notes"]].isna().all(axis=1)
    violations['rule_9'] = mask9
    mask10 = (df["drug unit"].notna() & df["drug type"].isna()) | (df["drug type"].notna() & df["drug unit"].isna())
    violations['rule_10'] = mask10
    return violations

def load_registry_info(campus_id, registry_path):
    df = pd.read_excel(registry_path, sheet_name="Sheet1")
    row = df[df["campus_id"] == campus_id]
    if row.empty:
        raise ValueError(f"Campus ID '{campus_id}' not found in hospital registry.")
    record = row.iloc[0]
    return {
        "healthcare_system": record["healthcare_system"].lower().replace(" ", "_"),
        "hospital_name": record["hospital_name"],
        "zip_code": str(record["zip_code"])
    }

def validate_negotiated_algorithm_format(df):
    if "negotiated algorithm" in df.columns:
        pattern = re.compile(r"^[0-9$%\\s]+$")
        df["negotiated_algorithm_invalid"] = df["negotiated algorithm"].astype(str).str.fullmatch(pattern).fillna(False)
    else:
        df["negotiated_algorithm_invalid"] = False
    return df

def remove_invalid_tokens(df):
    invalid_patterns = re.compile(r"^(n/?a|not applicable)$", re.IGNORECASE)
    for col in df.columns:
        if df[col].dtype == 'object':
            df.loc[df[col].str.fullmatch(invalid_patterns, na=False), col] = ""
    return df

def clean_price_fields(df):
    for col in PRICE_FIELDS:
        if col in df.columns:
            df[col] = (
            df[col].astype(str)
            .str.replace(r'[$%",]', '', regex=True)
            .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
            if col != "estimated amount":
                df.loc[df[col] <= 0, col] = pd.NA
                df.loc[df[col] == int(PLACEHOLDER_VALUE), col] = pd.NA

    return df

def normalize_text_fields(df):
    for col in TEXT_FIELDS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower().replace({"nan": ""})
    return df

def normalize_modifiers(df):
    if "modifiers" in df.columns:
        df["modifiers"] = df["modifiers"].apply(
            lambda x: (
                str(x).upper().replace("|", ",").replace(" ", "") if pd.notna(x) else pd.NA
            )
        )
    return df

def drop_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    logging.debug(f"Dropped {before - len(df)} duplicates in this chunk")
    return df

def validate_code_length(df):
    if "code" in df.columns and "code type" in df.columns:
        df["code"] = df["code"].astype(str)
        df["code type"] = df["code type"].astype(str).str.upper()

        ct = df["code type"]
        code = df["code"]

        valid_cpt    = (ct == "CPT")    & code.str.match(r"^\d{5}$")
        valid_hcpcs  = (ct == "HCPCS")  & (code.str.match(r"^\d{5}$") | code.str.match(r"^[A-V]\d{4}$"))
        valid_ndc    = (ct == "NDC")    & code.str.match(r"^\d{10,11}$")
        valid_drg    = (ct == "DRG")    & code.str.match(r"^\d{3}$")
        valid_cdt    = (ct == "CDT")    & code.str.match(r"^D\d{4}$")
        valid_apc    = (ct == "APC")    & code.str.match(r"^\d{4}$")
        valid_icd    = (ct == "ICD")    & code.str.len().between(3, 7)

        valid = valid_cpt | valid_icd | valid_ndc | valid_hcpcs | valid_drg | valid_cdt | valid_apc
        df = df[valid | df["code"].isna()]
    return df

def clean_large_file_in_chunks(input_path, healthcare_system, campus_id, base_dir=".", chunksize=100000):
    output_dir = os.path.join(base_dir, "data", "cleaned data", healthcare_system)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{campus_id}_cleaned.csv")

    devlog_dir = os.path.join(base_dir, "data", "logs", "devlogs", healthcare_system)
    os.makedirs(devlog_dir, exist_ok=True)
    dev_log_path = os.path.join(devlog_dir, f"{campus_id}_devlog.json")

    logs_dir = os.path.join(base_dir, "data", "logs")
    rule_violation_dir = os.path.join(logs_dir, "rules violations", healthcare_system)
    os.makedirs(rule_violation_dir, exist_ok=True)
    rule_csv_path = os.path.join(rule_violation_dir, f"{campus_id}_rules_violated.csv")

    if os.path.exists(output_path):
        os.remove(output_path)

    total_rows = 0
    total_violation_counts = {f"rule_{i}": 0 for i in range(1, 11)}
    total_algorithm_format_issues = 0
    total_duplicates_dropped = 0
    all_violated_rows = []

    for chunk_number, chunk in enumerate(pd.read_csv(input_path, dtype=str, chunksize=chunksize, low_memory=False), start=1):
        chunk.columns = chunk.columns.str.lower().str.strip()

        if "modifiers" not in chunk.columns:
            chunk["modifiers"] = pd.NA

        chunk = clean_price_fields(chunk)
        chunk = remove_invalid_tokens(chunk)
        chunk = normalize_text_fields(chunk)
        chunk = normalize_modifiers(chunk)
        chunk = validate_negotiated_algorithm_format(chunk)
        chunk = validate_code_length(chunk)

        before_dedup = len(chunk)
        chunk = drop_duplicates(chunk)
        after_dedup = len(chunk)
        total_duplicates_dropped += (before_dedup - after_dedup)

        violations = apply_conditional_rules(chunk)

        rule_tags = pd.Series([[] for _ in range(len(chunk))], index=chunk.index)
        for rule, mask in violations.items():
            total_violation_counts[rule] += int(mask.sum())
            rule_tags[mask] = rule_tags[mask].apply(lambda lst: lst + [rule])

        if rule_tags.notna().any():
            rule_df = chunk.copy()
            rule_df["rules_violated"] = rule_tags.apply(lambda lst: ",".join(lst) if lst else pd.NA)
            rule_df = rule_df[rule_df["rules_violated"].notna()]
            all_violated_rows.append(rule_df)
            # Drop those rows from the chunk
            violating_indices = rule_df.index
            chunk = chunk.drop(index=violating_indices)


        total_algorithm_format_issues += int(chunk["negotiated_algorithm_invalid"].sum())
        total_rows += len(chunk)

        if "transparency_score" in chunk.columns:
            chunk.drop(columns=["transparency_score"], inplace=True)
        if "negotiated_algorithm_invalid" in chunk.columns:
            chunk.drop(columns=["negotiated_algorithm_invalid"], inplace=True)

        chunk.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))

        logging.info(f"[{chunk_number}] Processed {len(chunk):,} rows")

    if all_violated_rows:
        violated_df = pd.concat(all_violated_rows)
        violated_df.to_csv(rule_csv_path, index=False)

    total_dropped_rows = sum(total_violation_counts.values())
    total_records_examined = total_rows + total_dropped_rows
    final_score = max(0, 1 - (sum(total_violation_counts.values()) / (total_records_examined * 10))) if total_records_examined else 0

    logging.info(f"Finished cleaning. Total rows: {total_rows:,}")
    logging.info(f"Duplicates dropped: {total_duplicates_dropped:,}")
    logging.info(f"Final Transparency Score: {final_score:.4f}")
    logging.info(f"Rule Violations Summary: {total_violation_counts}")
    logging.info(f"Negotiated Algorithm Format Violations: {total_algorithm_format_issues:,}")

    if os.path.exists(dev_log_path):
        with open(dev_log_path, "r") as f:
            devlog = json.load(f)
    else:
        devlog = {}

    devlog["cleaning_metadata"] = {
    "final_transparency_score": round(final_score, 4),
    "total_rows_cleaned": total_rows,
    "total_duplicates_dropped": total_duplicates_dropped,
    "total_rows_dropped_due_to_rule_violations": total_dropped_rows,
    "total_algorithm_format_violations": int(total_algorithm_format_issues),
    "rule_violations_summary": {k: int(v) for k, v in total_violation_counts.items()}
    }

    with open(dev_log_path, "w") as f:
        json.dump(devlog, f, indent=2)

    logging.info(f"Updated dev log saved to: {dev_log_path}")

    logging.info(f"\nCleaned CSV saved to:\n  {output_path} ({os.path.getsize(output_path) / (1024 * 1024):.2f} MB)")
    if os.path.exists(rule_csv_path):
        logging.info(f"Rules violations saved to:\n  {rule_csv_path}")

    return final_score, total_violation_counts, total_algorithm_format_issues

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clearcare Cleaner")
    parser.add_argument("--campus_id", required=True, help="Campus ID")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to hospital registry")
    parser.add_argument("--base_dir", default=".", help="Base directory")
    args = parser.parse_args()

    metadata = load_registry_info(args.campus_id, args.registry)
    healthcare_system = metadata["healthcare_system"]

    input_path = os.path.join(
        args.base_dir,
        "data", "extracted data", healthcare_system,
        f"{args.campus_id}_extracted.csv"
    )

    clean_large_file_in_chunks(
        input_path=input_path,
        healthcare_system=healthcare_system,
        campus_id=args.campus_id,
        base_dir=args.base_dir
    )
