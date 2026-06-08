import json
import argparse


def load_target_activities(txt_path):
    with open(txt_path, "r", encoding="utf-8") as f:
        return {
            line.strip()
            for line in f
            if line.strip()
        }


def contains_target_activity(entry, target_activities):
    # CHANGED: 'from_activity' and 'to_activity' are now 'source' and 'target'
    return (
        entry.get("source") in target_activities
        or entry.get("target") in target_activities
    )


def filter_json_by_activity_reference(
    input_json_path,
    activities_txt_path,
    output_json_path,
    exclude=False
):
    # Load target activities
    target_activities = load_target_activities(activities_txt_path)

    # Load JSON (CHANGED: The new JSON is just a list, not a dict with 'groups')
    with open(input_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Filter the list
    if exclude:
        # "Filter out" meaning REMOVE entries if they reference the txt file
        filtered_data = [
            entry for entry in data
            if not contains_target_activity(entry, target_activities)
        ]
    else:
        # "Filter out" meaning KEEP ONLY entries if they reference the txt file
        filtered_data = [
            entry for entry in data
            if contains_target_activity(entry, target_activities)
        ]

    # Save output (CHANGED: Dumping the flat list directly)
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(filtered_data, f, indent=2)

    print(f"Filtered JSON saved to: {output_json_path}")
    print(f"Original records: {len(data)} | Output records: {len(filtered_data)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Filter JSON records based on an activity set from a TXT file."
        )
    )

    parser.add_argument(
        "input_json",
        help="Input JSON file"
    )

    parser.add_argument(
        "activities_txt",
        help="TXT file with activities (one per line)"
    )

    parser.add_argument(
        "output_json",
        help="Output filtered JSON file"
    )

    # Added argument to give you control over whether you are keeping or removing
    parser.add_argument(
        "--exclude",
        action="store_true",
        help="If set, REMOVES records that match the TXT file instead of keeping them."
    )

    args = parser.parse_args()

    filter_json_by_activity_reference(
        args.input_json,
        args.activities_txt,
        args.output_json,
        exclude=args.exclude
    )