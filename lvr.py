from datetime import date
import requests
import zipfile
import os
import json
import pandas as pd
import glob
import csv


today = date.today()

if today.month in [1, 2, 3]:
    NEWEST_SEASON = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    NEWEST_SEASON = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    NEWEST_SEASON = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    NEWEST_SEASON = f"{today.year - 1911}S3"


def save_season_data(season=NEWEST_SEASON):
    url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={season}&type=zip&fileName=lvr_landcsv.zip"
    print("url:", url)

    response = requests.get(url)

    if response.status_code == 200:
        # Write the content of the response to the file
        with open(f"lvr_landcsv_{season}.zip", "wb") as file:
            file.write(response.content)
        print(f"Downloaded lvr_landcsv_{season}.zip")

        # Unzip
        with zipfile.ZipFile(f"lvr_landcsv_{season}.zip", "r") as zip_ref:
            zip_ref.extractall(f"lvr_landcsv_{season}")
        print(f"Unzipped lvr_landcsv_{season}")

        # Delete the zip file
        os.remove(f"lvr_landcsv_{season}.zip")
        print(f"Deleted lvr_landcsv_{season}.zip")

    else:
        print("Failed to download the file. Status code:", response.status_code)


def save_history_season_data(start="101S1", end=NEWEST_SEASON):
    while int(start[-1]) <= 4:
        # save_season_data(start)
        print(start)
        start = start[:-1] + str(int(start[-1]) + 1)
    start = str(int(start[:3]) + 1) + "S1"

    for year in range(int(start[:3]), int(end[:3]) + 1):
        for season in range(1, 5):
            if f"{year}S{season}" > end:
                break
            # save_season_data(f"{year}S{season}")
            print(f"{year}S{season}")


def merge_csv(data_dir_path, schema):
    data_file_paths = []
    for schema_file in schema["files"]:
        data_file_paths += glob.glob(
            os.path.join(data_dir_path, schema_file["pattern"])
        )

    season = data_dir_path[-5:]
    merge_file_path = f"lvr_land_{season}_{schema['schema_name']}.csv"
    print(f"Merge {merge_file_path}...")

    # Check if the merge file already exists and is non-empty
    merge_file_exists = (
        os.path.isfile(merge_file_path) and os.path.getsize(merge_file_path) > 0
    )

    with open(merge_file_path, "a", newline="") as merge_file:
        csv_writer = csv.writer(merge_file)

        for path in data_file_paths:
            print("", path)
            new_cols_dict = {}

            if "season" in schema["new_cols"]:
                new_cols_dict["season"] = season

            if "city" in schema["new_cols"]:
                new_cols_dict["city"] = path.split("/")[-1][0]

            if "category" in schema["new_cols"]:
                new_cols_dict["category"] = path.split("_")[-2]

            with open(path, "r", newline="") as current_file:
                csv_reader = csv.reader(current_file)

                # Skip header lines if the merge file already exists
                if merge_file_exists:
                    next(csv_reader)
                    next(csv_reader)

                else:
                    header1 = next(csv_reader)
                    header2 = next(csv_reader)

                    header1.extend(new_cols_dict.keys())
                    header2.extend(new_cols_dict.keys())

                    # Write the first two header lines to the merge file
                    csv_writer.writerow(header1)
                    # csv_writer.writerow(header2)

                    # After writing the first header, mark as existing
                    merge_file_exists = True

                for row in csv_reader:
                    if new_cols_dict:
                        for new_col_value in new_cols_dict.values():
                            row.append(new_col_value)

                    csv_writer.writerow(row)

        print("Merge done!\n")


if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)

    data_dir_paths = glob.glob("lvr_landcsv_???S?")
    # data_dir_paths = ["lvr_landcsv_113S1"]

    for data_dir_path in data_dir_paths:
        season = data_dir_path[-5:]
        print(f"Season: {season}\n")
        for schema in config["schemas"]:
            merge_csv(data_dir_path, schema)
