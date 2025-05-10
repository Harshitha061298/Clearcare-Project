import argparse
import os
import logging
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

from cleaning_utils import clean_large_file_in_chunks
from json_parser import parse_json
from tall_format_csv_extractor import extract_tall_format_csv
from wide_format_csv_extractor import extract_wide_format_csv

logging.basicConfig(
    filename='logs/devlog.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

EXTRACTOR_DISPATCH = {
    "json": lambda args: parse_json(
        campus_id=args.campus_id,
        registry_path="Hospital Registry.xlsx",
        config_path="utils/config.yaml",
        base_dir="."
    ),
    "tall csv": lambda args: extract_tall_format_csv(
        campus_id=args.campus_id,
        registry_path="Hospital Registry.xlsx",
        config_path="utils/config.yaml",
        base_dir="."
    ),
    "wide csv": lambda args: extract_wide_format_csv(
        campus_id=args.campus_id,
        registry_path="Hospital Registry.xlsx",
        config_path="utils/config.yaml",
        base_dir="."
    )
}

def load_registry(campus_id):
    registry = pd.read_excel("Hospital Registry.xlsx")
    record = registry.loc[registry["campus_id"] == campus_id].squeeze()
    return registry, record

def update_registry(registry, campus_id, updates):
    idx = registry[registry["campus_id"] == campus_id].index[0]
    for k, v in updates.items():
        registry.at[idx, k] = v
    registry.to_excel("Hospital Registry.xlsx", index=False)

def main():
    parser = argparse.ArgumentParser(description="Generalized Clearcare ETL Pipeline")
    parser.add_argument("--campus_id", required=True, help="Hospital campus ID")
    parser.add_argument("--user", required=True, help="Name of the user running this pipeline")
    parser.add_argument("--format", required=False, choices=["json", "tall csv", "wide csv"], help="Optional format override. If not provided, pulled from hospital registry")
    args = parser.parse_args()

    # Load hospital metadata
    registry, meta = load_registry(args.campus_id)
    hospital_name = meta.get("hospital_name", "Unknown")
    file_format = args.format if args.format else meta.get("structure")
    print(f"\n\033[1mStarting ETL process for {hospital_name}\033[0m")
    logging.info(f"Starting ETL for {hospital_name} ({args.campus_id})")

    # Phase 1: Extraction
    print("\nStarting extraction phase...")
    logging.info("Extraction phase started.")

    if file_format not in EXTRACTOR_DISPATCH:
        raise ValueError(f"Unsupported or missing format: {file_format}")

    EXTRACTOR_DISPATCH[file_format](args)

    # Phase 2: Cleaning
    print("\nStarting transforming phase...")
    logging.info("Cleaning/transforming phase started.")

    healthcare_system = meta["healthcare_system"].lower().replace(" ", "_")

    extracted_path = os.path.join("data", "extracted data", healthcare_system, f"{args.campus_id}_extracted.csv")
    devlog_path = os.path.join("data", "logs", healthcare_system, f"{args.campus_id}_devlog.json")

    clean_large_file_in_chunks(
        input_path=extracted_path,
        healthcare_system=healthcare_system,
        campus_id=args.campus_id,
        base_dir="."
    )

    # Final: Update Registry
    print("\nUpdating registry sheet with ETL metadata...")
    if os.path.exists(devlog_path):
        devlog = pd.read_json(devlog_path)
        latest_log = devlog.iloc[-1] if not devlog.empty else {}
        updates = {
            "hospital_address": latest_log.get("hospital_address", meta.get("hospital_address")),
            "version": latest_log.get("version", meta.get("version")),
            "last_updated_on": latest_log.get("last_updated_on", meta.get("last_updated_on")),
            "transparency_score": latest_log.get("transparency_score", meta.get("transparency_score")),
            "processed_by": args.user,
            "last_processed_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        update_registry(registry, args.campus_id, updates)

    print(f"\n\033[1mETL process completed for {hospital_name} ({args.campus_id})\033[0m")
    logging.info("ETL process complete.")

if __name__ == "__main__":
    main()
