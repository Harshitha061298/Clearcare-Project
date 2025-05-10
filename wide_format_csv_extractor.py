import pandas as pd
import os
import argparse
import json
import re
import logging
import yaml
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

HEADERS = [
    "hospital name", "zip code", "code", "code type", "description", "drug unit", "drug type",
    "insurance payer name", "insurance payer id", "insurance plan name",
    "negotiated price", "negotiated percentage", "negotiated algorithm", "negotiated methodology",
    "gross charge", "discounted cash price", "min price", "max price", "estimated amount",
    "setting", "additional notes", "modifiers"
]

STANDARD_CHARGE_PREFIXES = {
    "negotiated_dollar": "negotiated price",
    "negotiated_percentage": "negotiated percentage",
    "negotiated_algorithm": "negotiated algorithm",
    "estimated_amount": "estimated amount",
    "methodology": "negotiated methodology",
    "additional_payer_notes": "additional notes"
}

UNKNOWN_CODE_TYPES = defaultdict(int)
FIELD_PRESENCE_LOG = defaultdict(int)
CODE_TYPE_PRESENCE = defaultdict(int)
CODE_TYPE_MAPPINGS_USED = defaultdict(set)
MODIFIER_COUNTS = defaultdict(int)

def load_registry_info(campus_id, registry_path):
    df = pd.read_excel(registry_path, sheet_name="Sheet1")
    row = df[df["campus_id"] == campus_id]
    if row.empty:
        raise ValueError(f"Campus ID '{campus_id}' not found in hospital registry.")
    record = row.iloc[0]
    return {
        "hospital_name": record["hospital_name"],
        "zip_code": str(record["zip_code"]),
        "raw_filename": record["raw_filename"],
        "healthcare_system": record["healthcare_system"]
    }

def load_extract_config(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    extract_config = config.get("extract", {})
    modifier_map = config.get("modifiers", {})
    return (
        set(extract_config.get("allowed_code_types", [])),
        extract_config.get("code_type_normalization", {}),
        modifier_map
    )

def parse_column_for_payer(colname):
    parts = [p.strip() for p in colname.split("|")]
    if len(parts) < 3:
        return None, None, None
    return parts[0], parts[1], parts[2]  # field_type, payer_name, plan_name

def extract_wide_format_csv(campus_id, registry_path, config_path, base_dir):
    metadata = load_registry_info(campus_id, registry_path)
    allowed_code_types, code_type_map, modifier_map = load_extract_config(config_path)

    system = metadata["healthcare_system"].lower()
    raw_path = os.path.join(base_dir, "data", "raw data", system, metadata["raw_filename"])
    extracted_dir = os.path.join(base_dir, "data", "extracted data", system)
    devlog_dir = os.path.join(base_dir, "data", "logs", "devlogs", system)

    os.makedirs(extracted_dir, exist_ok=True)
    os.makedirs(devlog_dir, exist_ok=True)

    output_path = os.path.join(extracted_dir, f"{campus_id}_extracted.csv")
    dev_log_path = os.path.join(devlog_dir, f"{campus_id}_devlog.json")

    meta_df = pd.read_csv(raw_path, nrows=2, header=None).fillna("")
    mrf_metadata = dict(zip(meta_df.iloc[0], meta_df.iloc[1]))
    mrf_version = mrf_metadata.get("version", "")
    mrf_last_updated = mrf_metadata.get("last_updated_on", "")
    raw_hospital_location = mrf_metadata.get("hospital_location", "")
    raw_hospital_address = mrf_metadata.get("hospital_address", "")

    df = pd.read_csv(raw_path, skiprows=2, dtype=str).fillna("")
    all_columns = df.columns.tolist()
    payer_cols = [col for col in all_columns if col.count("|") >= 2 and (
        (col.count("|") == 2 and col.strip().split("|")[0].strip() in STANDARD_CHARGE_PREFIXES) or
        (col.count("|") > 2 and col.strip().split("|")[-1].strip() in STANDARD_CHARGE_PREFIXES)
    )]

    rows = []
    for _, row in df.iterrows():
        generic_notes = row.get("additional_generic_notes", "").strip()
        modifiers_raw = row.get("modifiers", "").strip()
        modifier_list = [m.strip() for m in re.split(r"[,|]", modifiers_raw) if m.strip()]
        for m in modifier_list:
            MODIFIER_COUNTS[m] += 1

        payer_row_map = defaultdict(dict)

        for col in payer_cols:
            parts = [p.strip() for p in col.split("|")]
            if len(parts) < 3:
                continue
            field_key = parts[0] if len(parts) == 3 else parts[-1]
            mapped_field = STANDARD_CHARGE_PREFIXES.get(field_key)
            if not mapped_field:
                continue

            _, payer_name, plan_name = parse_column_for_payer(col)
            if not payer_name:
                continue
            value = row[col]
            if value == "":
                continue

            for i in range(1, 5):
                code_col = f"code|{i}"
                type_col = f"code|{i}|type"
                code = row.get(code_col, "").strip()
                raw_code_type = row.get(type_col, "").strip().upper()
                if not code or not raw_code_type:
                    continue
                normalized_code_type = code_type_map.get(raw_code_type)
                CODE_TYPE_MAPPINGS_USED[raw_code_type].add(normalized_code_type)
                if normalized_code_type not in allowed_code_types:
                    UNKNOWN_CODE_TYPES[raw_code_type] += 1
                    continue
                CODE_TYPE_PRESENCE[normalized_code_type] += 1

                key = (code, normalized_code_type, payer_name, plan_name)
                payer_row_map[key][mapped_field] = value

        for (code, code_type, payer, plan), field_data in payer_row_map.items():
            additional_payer_note = field_data.pop("additional notes", "")
            combined_notes = ", ".join(filter(None, [
                generic_notes,
                additional_payer_note
            ]))

            record = {
                "hospital name": metadata["hospital_name"],
                "zip code": metadata["zip_code"],
                "code": code,
                "code type": code_type,
                "insurance payer name": payer,
                "insurance plan name": plan,
                "negotiated price": field_data.get("negotiated price", ""),
                "negotiated percentage": field_data.get("negotiated percentage", ""),
                "negotiated algorithm": field_data.get("negotiated algorithm", ""),
                "negotiated methodology": field_data.get("negotiated methodology", ""),
                "estimated amount": field_data.get("estimated amount", ""),
                "drug unit": row.get("drug_unit_of_measurement", ""),
                "drug type": row.get("drug_type_of_measurement", ""),
                "description": row.get("description", ""),
                "gross charge": row.get("standard_charge|gross", ""),
                "discounted cash price": row.get("standard_charge|discounted_cash", ""),
                "min price": row.get("standard_charge|min", ""),
                "max price": row.get("standard_charge|max", ""),
                "setting": row.get("setting", ""),
                "additional notes": combined_notes,
                "modifiers": modifiers_raw
            }

            for k in record:
                if record[k]:
                    FIELD_PRESENCE_LOG[k] += 1

            rows.append(record)

    extracted_df = pd.DataFrame(rows, columns=HEADERS)
    extracted_df.to_csv(output_path, index=False)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    logging.info(f"Parsing done. Extracted {len(extracted_df):,} records into '{output_path}' with size: {size_mb:.2f} MB")

    full_field_summary = {field: FIELD_PRESENCE_LOG.get(field, 0) for field in HEADERS}
    missing_code_types = [ct for ct in allowed_code_types if CODE_TYPE_PRESENCE[ct] == 0]

    devlog = {
        "payer_columns_parsed": len(payer_cols),
        "total_rows_extracted": len(extracted_df),
        "raw_address_info": {
            "hospital_location": raw_hospital_location,
            "hospital_address": raw_hospital_address
        },
        "mrf_metadata": {
            "version": mrf_version,
            "last_updated_on": mrf_last_updated
        },
        "field_presence_summary": full_field_summary,
        "unrecognized_code_types": dict(UNKNOWN_CODE_TYPES),
        "missing_code_types": missing_code_types,
        "code_type_presence": dict(CODE_TYPE_PRESENCE),
        "code_type_normalizations_used": {k: list(v) for k, v in CODE_TYPE_MAPPINGS_USED.items()},
        "modifier_counts": dict(MODIFIER_COUNTS),
    }

    with open(dev_log_path, "w") as f:
        json.dump(devlog, f, indent=2)
    logging.info(f"Dev log saved to: {dev_log_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clearcare Wide Format CSV Extractor")
    parser.add_argument("--campus_id", required=True, help="Campus ID as per Hospital Registry")
    parser.add_argument("--registry", default="Hospital Registry.xlsx")
    parser.add_argument("--config", default="utils/config.yaml")
    parser.add_argument("--base_dir", default=".")
    args = parser.parse_args()

    extract_wide_format_csv(
        campus_id=args.campus_id,
        registry_path=args.registry,
        config_path=args.config,
        base_dir=args.base_dir
    )
