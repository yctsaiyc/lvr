from datetime import date
import requests
import zipfile
import os
import json
import pandas as pd
import glob
import csv


with open("config.json", "r") as f:
    config = json.load(f)

today = date.today()

if today.month in [1, 2, 3]:
    NEWEST_SEASON = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    NEWEST_SEASON = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    NEWEST_SEASON = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    NEWEST_SEASON = f"{today.year - 1911}S3"


def save_season_raw_data(raw_dir_parent_path="data/raw", season=NEWEST_SEASON):
    url = config["url"] + season
    print("url:", url)

    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs(raw_dir_parent_path, exist_ok=True)

        # Write the content of the response to the file
        with open(f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip", "wb") as file:
            file.write(response.content)
        print(f"Downloaded lvr_landcsv_{season}.zip")

        # Unzip
        with zipfile.ZipFile(
            f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip", "r"
        ) as zip_ref:
            zip_ref.extractall(f"{raw_dir_parent_path}/lvr_landcsv_{season}")
        print(f"Unzipped lvr_landcsv_{season}")

        # Delete the zip file
        os.remove(f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip")
        print(f"Deleted lvr_landcsv_{season}.zip")

    else:
        print("Failed to download the file. Status code:", response.status_code)


def save_history_season_raw_data(
    raw_dir_parent_path="data/raw", start="101S1", end=NEWEST_SEASON
):
    while int(start[-1]) <= 4:
        print(start)
        save_season_raw_data(raw_dir_parent_path, start)
        start = start[:-1] + str(int(start[-1]) + 1)
    start = str(int(start[:3]) + 1) + "S1"

    for year in range(int(start[:3]), int(end[:3]) + 1):
        for season in range(1, 5):
            if f"{year}S{season}" > end:
                break
            print(f"{year}S{season}")
            save_season_raw_data(raw_dir_parent_path, f"{year}S{season}")


def merge_csv(
    schema="main",
    raw_dir_path="data/raw/lvr_landcsv_101S1",
    merged_dir_path="data/merged",
):
    os.makedirs(merged_dir_path, exist_ok=True)
    schema_dict = config["schemas"][schema]

    raw_file_paths = []
    for schema_file in schema_dict["files"]:
        raw_file_paths += glob.glob(os.path.join(raw_dir_path, schema_file["pattern"]))

    season = raw_dir_path[-5:]  # yyySs
    merged_file_path = f"{merged_dir_path}/lvr_land_{season}_{schema}.csv"
    print(f"Merge {merged_file_path}...")

    # Check if the merge file already exists and is non-empty
    merged_file_exists = (
        os.path.isfile(merged_file_path) and os.path.getsize(merged_file_path) > 0
    )

    # If the merge file already exists, ask the user if they want to overwrite it
    if merged_file_exists:
        action = input(f"Merge file already exists. Overwrite it? (y/n): ").lower()

        if action == "y" or "yes":
            os.remove(merged_file_path)
            merged_file_exists = False

        elif action == "n" or "no":
            return

        else:
            print("Invalid action.")
            return

    # Create the merge file
    with open(merged_file_path, "a", newline="") as merged_file:
        csv_dict_writer = csv.DictWriter(merged_file, fieldnames=schema_dict["fields"])
        csv_dict_writer.writeheader()

        for path in raw_file_paths:
            print("", path)

            # Read the CSV file
            with open(path, "r", newline="") as current_file:

                # Remove Byte Order Mark ("\ufeff") if it exists
                first_line = current_file.readline()

                if first_line.startswith("\ufeff"):
                    first_line = first_line.lstrip("\ufeff")
                    current_file = [first_line] + current_file.readlines()

                else:
                    current_file.seek(0)

                # Read the CSV file
                csv_dict_reader = csv.DictReader(current_file)

                # Skip English header
                next(csv_dict_reader)

                # Write the rows to the merge file
                for row_idx, row_dict in enumerate(csv_dict_reader):
                    try:
                        csv_dict_writer.writerow(
                            process_data_row_dict(
                                row_idx, row_dict, schema_dict["fields"], season, path
                            )
                        )
                    except ValueError:
                        print("  ValueError on row:", row_idx + 3)
                        failed_file_path = f"{merged_dir_path}/failed_{schema}.csv"

                        # Write the row to file
                        with open(failed_file_path, "a", newline="") as failed_file:
                            csv_writer = csv.writer(failed_file)

                            # Write the header if the file is new
                            if failed_file.tell() == 0:
                                csv_writer.writerow(schema_dict["fields"].keys())

                            row = []
                            for field in schema_dict["fields"]:
                                if schema_dict["fields"][field] in [
                                    "season",
                                    "city",
                                    "category",
                                ]:
                                    row.append(row_dict[field])

                                else:
                                    row += (
                                        current_file[row_idx + 2]
                                        .replace("\r\n", "")
                                        .split(",")
                                    )
                                    csv_writer.writerow(row)
                                    break

                        print(f"   Saved in {merged_dir_path}/failed_{schema}.csv")

        print("Merge done!\n")


def merge_csv_all_schemas(
    raw_dir_path="data/raw/lvr_landcsv_???S?", merged_dir_path="data/merged"
):
    raw_dir_paths = glob.glob(raw_dir_path)

    for raw_dir_path in raw_dir_paths:
        season = raw_dir_path[-5:]
        print(f"Season: {season}\n")
        for schema in config["schemas"]:
            merge_csv(schema, raw_dir_path, merged_dir_path)


def process_data_row_dict(row_idx, row_dict, fields, season, raw_file_path):
    for field in fields:
        field_type = fields[field]

        if field_type == "season":
            row_dict[field] = season.replace("S", "Q")

        elif field_type == "city":
            code = raw_file_path.split("/")[-1][0]
            row_dict[field] = config["code_mappings"]["city"][code]

        elif field_type == "category":
            code = raw_file_path.split("_")[-2]
            row_dict[field] = config["code_mappings"]["category"][code]

        elif field_type == "date" and row_dict[field] != "":
            try:
                day = row_dict[field][-2:]
                month = row_dict[field][-4:-2]
                year = int(row_dict[field][:-4]) + 1911
                row_dict[field] = f"{year}-{month}-{day}"
            except Exception as e:
                print(
                    f"  Row {row_idx + 3} in column {field}: {row_dict[field]} has been replaced with an empty string."
                )
                row_dict[field] = ""

        # Standardize field names with changed names
        elif field_type == "rm_parens":
            if field not in row_dict:
                row_dict[field] = row_dict.pop(field[:-4] + "(" + field[-4:] + ")")

    return row_dict
