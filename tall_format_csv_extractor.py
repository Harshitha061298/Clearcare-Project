import pandas as pd
import os
import argparse
import json
import re
import logging
import yaml
from collections import defaultdict
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

HEADERS = [
    "hospital name", "zip code", "code", "code type", "description", "drug unit", "drug type",
    "insurance payer name", "insurance payer id", "insurance plan name",
    "negotiated price", "negotiated percentage", "negotiated algorithm", "negotiated methodology",
    "gross charge", "discounted cash price", "min price", "max price", "estimated amount",
    "setting", "additional notes", "modifiers"
]

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
    return (
        set(extract_config.get("allowed_code_types", [])),
        extract_config.get("code_type_normalization", {}),
    )

def extract_tall_format_csv(campus_id, registry_path, config_path, base_dir):
    metadata = load_registry_info(campus_id, registry_path)
    allowed_code_types, code_type_map = load_extract_config(config_path)

    system = metadata["healthcare_system"].lower()
    raw_path = os.path.join(base_dir, "data", "raw data", f"{system}", metadata["raw_filename"])
    extracted_dir = os.path.join(base_dir, "data", "extracted data", f"{system}")
    devlog_dir = os.path.join(base_dir, "data", "logs", "devlogs", system)

    os.makedirs(extracted_dir, exist_ok=True)
    os.makedirs(devlog_dir, exist_ok=True)

    output_path = os.path.join(extracted_dir, f"{campus_id}_extracted.csv")
    dev_log_path = os.path.join(devlog_dir, f"{campus_id}_devlog.json")

    written = 0

    metadata_df = pd.read_csv(raw_path, nrows=2, header=None).fillna("")
    mrf_metadata_dict = dict(zip(metadata_df.iloc[0], metadata_df.iloc[1]))
    mrf_version = mrf_metadata_dict.get("version", "")
    mrf_last_updated = mrf_metadata_dict.get("last_updated_on", "")
    raw_hospital_location = mrf_metadata_dict.get("hospital_location", "")
    raw_hospital_address = mrf_metadata_dict.get("hospital_address", "")

    with open(output_path, 'w', newline='', encoding='utf-8') as out_csv:
        out_csv.write(",".join(HEADERS) + "\n")

        for chunk in pd.read_csv(raw_path, skiprows=2, chunksize=100000, dtype=str, low_memory=False):
            chunk = chunk.replace(np.nan, "", regex=True)

            for _, row in chunk.iterrows():
                payer_key = "payer_name"
                payer = row.get(payer_key, "")

                payer_name, payer_id = payer, ""
                if match := re.search(r"(.*)\[(.*?)\]", payer):
                    payer_name = match.group(1).strip()
                    payer_id = match.group(2).strip()

                notes_key = "additional_generic_notes"
                notes = row.get(notes_key, "")

                modifiers_key = "modifiers"
                modifiers_raw = row.get(modifiers_key, "").strip()
                modifier_list = [m.strip() for m in re.split(r"[,|]", modifiers_raw) if m.strip()]
                for m in modifier_list:
                    MODIFIER_COUNTS[m] += 1

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

                    output_row = {
                        "hospital name": metadata["hospital_name"],
                        "zip code": metadata["zip_code"],
                        "code": code,
                        "code type": normalized_code_type,
                        "description": row.get("description", ""),
                        "drug unit": row.get("drug_unit_of_measurement", ""),
                        "drug type": row.get("drug_type_of_measurement", ""),
                        "insurance payer name": payer_name,
                        "insurance payer id": payer_id,
                        "insurance plan name": row.get("plan_name", ""),
                        "negotiated price": row.get("standard_charge|negotiated_dollar", ""),
                        "negotiated percentage": row.get("standard_charge|negotiated_percentage", ""),
                        "negotiated algorithm": row.get("standard_charge|negotiated_algorithm", ""),
                        "negotiated methodology": row.get("standard_charge|methodology", ""),
                        "gross charge": row.get("standard_charge|gross", ""),
                        "discounted cash price": row.get("standard_charge|discounted_cash", ""),
                        "min price": row.get("standard_charge|min", ""),
                        "max price": row.get("standard_charge|max", ""),
                        "estimated amount": row.get("estimated_amount", ""),
                        "setting": row.get("setting", ""),
                        "additional notes": notes,
                        "modifiers": modifiers_raw
                    }

                    for key in HEADERS:
                        if output_row.get(key):
                            FIELD_PRESENCE_LOG[key] += 1

                    out_csv.write(",".join([f'"{output_row.get(col, "").replace("\"", "''")}"' for col in HEADERS]) + "\n")
                    written += 1

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    logging.info(f" Parsing done. Extracted {written:,} records into '{output_path}' with size: {size_mb:.2f} MB")

    full_field_summary = {field: FIELD_PRESENCE_LOG.get(field, 0) for field in HEADERS}
    missing_code_types = [ct for ct in allowed_code_types if CODE_TYPE_PRESENCE[ct] == 0]

    with open(dev_log_path, "w") as log_file:
        json.dump({
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
        }, log_file, indent=2)
    logging.info(f" Dev log saved to: {dev_log_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clearcare Tall Format CSV Extractor")
    parser.add_argument("--campus_id", required=True, help="Campus ID as per Hospital Registry")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to hospital registry Excel file")
    parser.add_argument("--config", default="utils/config.yaml", help="Path to config YAML file")
    parser.add_argument("--base_dir", default=".", help="Base directory of Clearcare project")
    args = parser.parse_args()

    extract_tall_format_csv(
        campus_id=args.campus_id,
        registry_path=args.registry,
        config_path=args.config,
        base_dir=args.base_dir
    )
