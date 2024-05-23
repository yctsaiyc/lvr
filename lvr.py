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
        # Write the content of the response to the file
        with open(f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip", "wb") as file:
            file.write(response.content)
        print(f"Downloaded lvr_landcsv_{season}.zip")

        # Unzip
        with zipfile.ZipFile(f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip", "r") as zip_ref:
            zip_ref.extractall(f"{raw_dir_parent_path}/lvr_landcsv_{season}")
        print(f"Unzipped lvr_landcsv_{season}")

        # Delete the zip file
        os.remove(f"{raw_dir_parent_path}/lvr_landcsv_{season}.zip")
        print(f"Deleted lvr_landcsv_{season}.zip")

    else:
        print("Failed to download the file. Status code:", response.status_code)


def save_history_season_raw_data(raw_dir_parent_path="data/raw", start="101S1", end=NEWEST_SEASON):
    while int(start[-1]) <= 4:
        save_season_raw_data(raw_dir_parent_path, start)
        print(start)
        start = start[:-1] + str(int(start[-1]) + 1)
    start = str(int(start[:3]) + 1) + "S1"

    for year in range(int(start[:3]), int(end[:3]) + 1):
        for season in range(1, 5):
            if f"{year}S{season}" > end:
                break
            save_season_raw_data(raw_dir_parent_path, f"{year}S{season}")
            print(f"{year}S{season}")


def merge_csv(schema="main", raw_dir_path="data/raw/lvr_landcsv_101S1", merged_dir_path="data/merged"):
    schema_dict = config["schemas"][schema]

    raw_file_paths = []
    for schema_file in schema_dict["files"]:
        raw_file_paths += glob.glob(
            os.path.join(raw_dir_path, schema_file["pattern"])
        )

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
        csv_writer = csv.writer(merged_file)

        for path in raw_file_paths:
            print("", path)

            new_cols_dict = {}

            if "season" in schema_dict["new_cols"]:
                new_cols_dict["season"] = season

            if "city" in schema_dict["new_cols"]:
                new_cols_dict["city"] = path.split("/")[-1][0]

            if "category" in schema_dict["new_cols"]:
                new_cols_dict["category"] = path.split("_")[-2]

            # Read the CSV file
            with open(path, "r", newline="") as current_file:
                csv_reader = csv.reader(current_file)

                # Skip header lines if the merge file already exists
                if merged_file_exists:
                    next(csv_reader)
                    next(csv_reader)

                else:
                    header1 = next(csv_reader)
                    header2 = next(csv_reader)

                    header1.extend(new_cols_dict.keys())
                    header2.extend(new_cols_dict.keys())

                    # Write the header lines to the merge file
                    csv_writer.writerow(header1)
                    # csv_writer.writerow(header2)

                    # After writing the first header, mark as existing
                    merged_file_exists = True

                for row in csv_reader:
                    if new_cols_dict:
                        for new_col_value in new_cols_dict.values():
                            row.append(new_col_value)

                    csv_writer.writerow(row)

        print("Merge done!\n")


def merge_csv_all_schemas(raw_dir_path="data/raw/lvr_landcsv_???S?", merged_dir_path="data/merged"):
    raw_dir_paths = glob.glob(raw_dir_path)

    for raw_dir_path in raw_dir_paths:
        season = raw_dir_path[-5:]
        print(f"Season: {season}\n")
        for schema in config["schemas"]:
            merge_csv(schema, raw_dir_path, merged_dir_path)


def process_date(file_path="data/merged/lvr_land_???S?_*.csv"):
    file_paths = glob.glob(file_path)

    for file in file_paths:
        print(f"Processing date in {file}...")
        schema = file.split(".")[0].split("_")[-1]
        schema_dict = config["schemas"][schema]

        # Read the CSV file
        with open(file, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            fieldnames = reader.fieldnames  # Get the column names

        # Modify the specified columns
        for idx, row in enumerate(rows):
            for col in schema_dict["date_cols"]:
                if col in row and row[col] != "":
                    try:
                        day = row[col][-2:]
                        month = row[col][-4:-2]
                        year = int(row[col][:-4]) + 1911
                        row[col] = f"{year}-{month}-{day}"
                    except Exception as e:
                        print(f"Error processing row {idx + 2} in column {col}: {row[col]}")

        # Write the modified data back to the CSV file
        with open(file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print("Done!")
