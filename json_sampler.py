import os
import json
import argparse
import logging
import pandas as pd

def load_registry_info(campus_id, registry_path):
    df = pd.read_excel(registry_path, sheet_name="Sheet1")
    row = df[df["campus_id"] == campus_id]
    if row.empty:
        raise ValueError(f"Campus ID '{campus_id}' not found in hospital registry.")
    record = row.iloc[0]
    return {
        "healthcare_system": record["healthcare_system"],
        "raw_filename": record["raw_filename"]
    }

def create_sample(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as infile:
            data = json.load(infile)

        sample = {
            "hospital_name": data.get("hospital_name", "Not Found"),
            "hospital_location": data.get("hospital_location", "Not Found"),
            "hospital_address": data.get("hospital_address", "Not Found"),
            "last_updated_on": data.get("last_updated_on", "Not Found"),
            "version": data.get("version", "Not Found"),
            "license_information": data.get("license_information", "Not Found"),
            "affirmation": data.get("affirmation", "Not Found"),
            "standard_charge_information_sample": data.get("standard_charge_information", [])[:100],
            "modifier_information_sample": data.get("modifier_information", [])[:50]
        }

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(sample, outfile, indent=2)

        logging.info(f"Sample created successfully: {output_file}")
        print(f"Sample created successfully: {output_file}")

    except Exception as e:
        logging.error(f"Failed to create sample: {e}")
        print(f"Error creating sample: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample JSON Extractor for Hospitals")
    parser.add_argument("--campus_id", required=True, help="Campus ID as per Hospital Registry")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to hospital registry Excel file")
    parser.add_argument("--base_dir", default=".", help="Base directory of Clearcare project")

    args = parser.parse_args()

    registry_info = load_registry_info(args.campus_id, args.registry)
    system = registry_info["healthcare_system"].lower()
    input_file = os.path.join(args.base_dir, "data", "raw data", system, registry_info["raw_filename"])
    output_file = os.path.join(args.base_dir, "data", "extracted data", "sample json", system, f"{args.campus_id}_sample.json")

    os.makedirs(os.path.join(args.base_dir, "logs"), exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(args.base_dir, "logs", f"{args.campus_id}_sample_log.txt"),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    create_sample(input_file, output_file)
