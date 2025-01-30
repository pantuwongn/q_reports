import argparse
from data_processor import get_data_by_datetime
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Fetch data for a given datetime or list of datetimes.")
    parser.add_argument("--mode", choices=["current", "list"], required=True,
                        help="Mode: 'current' for now, 'list' for specific timestamps.")
    parser.add_argument("--timestamps", nargs="*",
                        help="List of timestamps (YYYY-MM-DD HH:MM:SS format). Required for 'list' mode.")

    args = parser.parse_args()

    if args.mode == "current":
        data_dict_list = get_data_by_datetime()
    elif args.mode == "list":
        if not args.timestamps:
            print("Error: --timestamps is required when mode is 'list'.")
            return
        data_dict_list = get_data_by_datetime(args.timestamps)

    df = pd.DataFrame(data_dict_list)
    df.to_csv('reports.csv', index=False)


if __name__ == "__main__":
    main()
