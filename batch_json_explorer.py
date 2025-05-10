import os
import pandas as pd
import argparse
import logging
from json_explorer import extract_keys_ijson, save_output


def batch_explore_by_system(healthcare_system, registry_path, base_dir):
    registry_path = os.path.abspath(registry_path)
    raw_dir = os.path.join(base_dir, "data", "raw data", healthcare_system)
    output_dir = os.path.join(base_dir, "data", "extracted data", "json structure", healthcare_system)
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, f"{healthcare_system}_batch_explorer_log.txt"),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        df = pd.read_excel(registry_path)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    except Exception as e:
        logging.error(f"Failed to load Hospital Registry: {e}")
        return

    matched = df[df['healthcare_system'].str.lower() == healthcare_system.lower()]

    for _, row in matched.iterrows():
        campus_id = str(row.get('campus_id', '')).strip()
        raw_filename = str(row.get('raw_filename', '')).strip()
        if not campus_id or not raw_filename:
            continue

        raw_file_path = os.path.join(raw_dir, raw_filename)
        if not os.path.exists(raw_file_path):
            logging.warning(f"Raw file not found: {raw_file_path}")
            continue

        try:
            structure = extract_keys_ijson(raw_file_path)
            out_path = os.path.join(output_dir, f"{campus_id}_structure.txt")
            save_output(out_path, structure)
            logging.info(f"Extracted: {raw_file_path} -> {out_path}")
        except Exception as e:
            logging.error(f"Failed to extract keys from {raw_file_path}: {e}")

    logging.info("Batch JSON structure extraction completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch JSON Key Structure Extractor by Healthcare System")
    parser.add_argument("--healthcare_system", required=True, help="Name of the healthcare system")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to the hospital registry Excel file")
    parser.add_argument("--base_dir", default=".", help="Base directory of the Clearcare project")
    args = parser.parse_args()

    batch_explore_by_system(args.healthcare_system, args.registry, args.base_dir)
    print("Batch extraction complete. Check logs for details.")
