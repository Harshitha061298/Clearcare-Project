import ijson
import argparse
import os
import pandas as pd

def extract_keys_ijson(input_file):
    result = ""
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        parser = ijson.parse(f)
        seen = set()
        for prefix, event, value in parser:
            if prefix not in seen:
                seen.add(prefix)
                indent = prefix.count('.')
                key_name = prefix.split('.')[-1]
                result += "    " * indent + f"- {key_name} ({event})\n"

    return result

def save_output(output_path, content):
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

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

def main():
    parser = argparse.ArgumentParser(description="Explore JSON (streaming) and output key structure.")
    parser.add_argument("--campus_id", required=True, help="Campus ID as per Hospital Registry")
    parser.add_argument("--registry", default="Hospital Registry.xlsx", help="Path to hospital registry Excel file")
    parser.add_argument("--base_dir", default=".", help="Base directory of Clearcare project")
    args = parser.parse_args()

    registry_info = load_registry_info(args.campus_id, args.registry)
    system = registry_info["healthcare_system"].lower()
    raw_path = os.path.join(args.base_dir, "data", "raw data", system, registry_info["raw_filename"])
    output_path = os.path.join(args.base_dir, "data", "extracted data", "json structure", system, f"{args.campus_id}_structure.txt")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    keys_structure = extract_keys_ijson(raw_path)
    save_output(output_path, keys_structure)

    print(f"JSON structure extracted and saved to: {output_path}")

if __name__ == "__main__":
    main()
