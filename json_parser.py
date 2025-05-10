import ijson
import csv
import argparse
import os
import re
import logging
import json
import yaml
import pandas as pd
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

HEADERS = [
    "hospital name", "zip code", "code", "code type", "description", "drug unit", "drug type",
    "insurance payer name", "insurance payer id", "insurance plan name",
    "negotiated price", "negotiated percentage", "negotiated algorithm", "negotiated methodology",
    "gross charge", "discounted cash price", "min price", "max price", "estimated amount",
    "setting", "additional notes"
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
        extract_config.get("code_type_normalization", {})
    )


def parse_json(campus_id, registry_path, config_path, base_dir):
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

    with open(output_path, 'w', newline='', encoding='utf-8') as out_csv:
        writer = csv.DictWriter(out_csv, fieldnames=HEADERS)
        writer.writeheader()

        written = 0
        raw_hospital_location = None
        raw_hospital_address = None
        raw_top_level_keys = set()
        mrf_version = None
        mrf_last_updated = None

        with open(raw_path, 'r', encoding='utf-8-sig') as f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                if prefix == 'hospital_location.item' and event == 'string':
                    raw_hospital_location = value
                if prefix == 'hospital_address.item' and event == 'string':
                    raw_hospital_address = value
                if raw_hospital_location and raw_hospital_address:
                    break

        with open(raw_path, 'r', encoding='utf-8-sig') as f:
            root = json.load(f)

        raw_top_level_keys = set(root.keys())
        known_keys_used = {"standard_charge_information", "hospital_location", "hospital_address", "modifier_information"}

        mrf_version = root.get("version")
        mrf_last_updated = root.get("last_updated_on")

        sci = root.get("standard_charge_information", [])
        if isinstance(sci, dict):
            items = sci.get("item", [])
        elif isinstance(sci, list):
            items = sci
        else:
            items = []

        for item in items:
            description = item.get("description", "")
            code_info = item.get("code_information", [])
            charge_entries = item.get("standard_charges", [])
            drug_info = item.get("drug_information", {})

            has_valid_code = False

            for code_entry in code_info:
                raw_code_type = str(code_entry.get("type", "")).strip().upper()
                normalized_code_type = code_type_map.get(raw_code_type)
                CODE_TYPE_MAPPINGS_USED[raw_code_type].add(normalized_code_type)

                if normalized_code_type not in allowed_code_types:
                    UNKNOWN_CODE_TYPES[raw_code_type] += 1
                    continue

                CODE_TYPE_PRESENCE[normalized_code_type] += 1
                has_valid_code = True
                code = code_entry.get("code", "")

                for charge in charge_entries:
                    gross_charge = charge.get("gross_charge", "")
                    discounted_cash = charge.get("discounted_cash", "")
                    min_price = charge.get("minimum", "")
                    max_price = charge.get("maximum", "")
                    setting = charge.get("setting", "")

                    for payer in charge.get("payers_information", []):
                        estimated_amount = payer.get("estimated_amount") or drug_info.get("estimated_amount", "")
                        negotiated_price = payer.get("standard_charge_dollar", "")
                        negotiated_percentage = payer.get("standard_charge_percentage", "")
                        negotiated_algorithm = payer.get("standard_charge_algorithm", "")
                        negotiated_methodology = payer.get("negotiated_methodology", "")

                        row = {
                            "hospital name": metadata["hospital_name"],
                            "zip code": metadata["zip_code"],
                            "code": code,
                            "code type": normalized_code_type,
                            "description": description,
                            "drug unit": drug_info.get("unit", ""),
                            "drug type": drug_info.get("type", ""),
                            "insurance payer name": payer.get("payer_name", ""),
                            "insurance payer id": payer.get("payer_id", ""),
                            "insurance plan name": payer.get("plan_name", ""),
                            "negotiated price": negotiated_price,
                            "negotiated percentage": negotiated_percentage,
                            "negotiated algorithm": negotiated_algorithm,
                            "negotiated methodology": negotiated_methodology,
                            "gross charge": gross_charge,
                            "discounted cash price": discounted_cash,
                            "min price": min_price,
                            "max price": max_price,
                            "estimated amount": estimated_amount,
                            "setting": setting,
                            "additional notes": payer.get("additional_payer_notes", "")
                        }

                        for key in HEADERS:
                            if row.get(key):
                                FIELD_PRESENCE_LOG[key] += 1

                        writer.writerow(row)
                        written += 1

        # Modifier-only records (standalone, outside standard_charge_information)
        for mod in root.get("modifier_information", []):
            mod_code = mod.get("code", "")
            mod_desc = mod.get("description", "")
            MODIFIER_COUNTS[mod_code] += 1
            for payer in mod.get("modifier_payer_information", []):
                row = {
                    "hospital name": metadata["hospital_name"],
                    "zip code": metadata["zip_code"],
                    "code": mod_code,
                    "code type": "MODIFIER",
                    "description": mod_desc,
                    "insurance payer name": payer.get("payer_name", ""),
                    "insurance payer id": "",
                    "insurance plan name": payer.get("plan_name", ""),
                    "negotiated price": "",
                    "negotiated percentage": "",
                    "negotiated algorithm": "",
                    "negotiated methodology": "",
                    "gross charge": "",
                    "discounted cash price": "",
                    "min price": "",
                    "max price": "",
                    "estimated amount": "",
                    "setting": "",
                    "additional notes": payer.get("description", "")
                }
                for key in HEADERS:
                    if row.get(key):
                        FIELD_PRESENCE_LOG[key] += 1
                writer.writerow(row)
                written += 1

    if written == 0:
        logging.warning(" No valid records found for allowed code types.")
    else:
        size_mb = os.path.getsize(output_path) / 1024 / 1024
        logging.info(f" Parsing done. Extracted {written:,} records into '{output_path}' with size: {size_mb:.2f} MB")

    # Ensure every field in HEADERS has a count (even 0)
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
            "unused_optional_json_keys": sorted(list(raw_top_level_keys - known_keys_used)),
        }, log_file, indent=2)
    logging.info(f" Dev log saved to: {dev_log_path }")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clearcare JSON Parser")
    parser.add_argument("--campus_id", required=True, help="Campus ID as per Hospital Registry")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to hospital registry Excel file")
    parser.add_argument("--config", default="utils/config.yaml", help="Path to config YAML file")
    parser.add_argument("--base_dir", default=".", help="Base directory of Clearcare project")

    args = parser.parse_args()

    parse_json(
        campus_id=args.campus_id,
        registry_path=args.registry,
        config_path=args.config,
        base_dir=args.base_dir
    )
