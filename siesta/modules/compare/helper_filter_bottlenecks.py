import json
import argparse


def load_allowed_activities(txt_path):
    with open(txt_path, "r", encoding="utf-8") as f:
        return {
            line.strip()
            for line in f
            if line.strip()
        }


def filter_json(input_json_path, activities_txt_path, output_json_path):
    # Load allowed activities
    allowed = load_allowed_activities(activities_txt_path)

    # Load JSON
    with open(input_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    filtered_groups = {}

    # Iterate through groups
    for group_name, entries in data.get("groups", {}).items():

        filtered_entries = [
            entry
            for entry in entries
            if entry.get("from_activity") in allowed
            and entry.get("to_activity") in allowed
        ]

        # Keep only non-empty groups
        if filtered_entries:
            filtered_groups[group_name] = filtered_entries

    # Build final output
    filtered_data = {
        "global_stats": data.get("global_stats", {}),
        "groups": filtered_groups
    }

    # Save output
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(filtered_data, f, indent=2)

    print(f"Filtered JSON saved to: {output_json_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter JSON entries based on allowed activities."
    )

    parser.add_argument(
        "input_json",
        help="Path to input JSON file"
    )

    parser.add_argument(
        "activities_txt",
        help="TXT file containing allowed activity names (one per line)"
    )

    parser.add_argument(
        "output_json",
        help="Path to output filtered JSON"
    )

    args = parser.parse_args()

    filter_json(
        args.input_json,
        args.activities_txt,
        args.output_json
    )