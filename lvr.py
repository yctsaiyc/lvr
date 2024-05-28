from datetime import date
import requests
import zipfile
import os
import json
import pandas as pd
import glob
import csv
import re


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
                csv_dict_reader = csv.DictReader(current_file, quotechar="\x07")

                # Skip English header
                next(csv_dict_reader)

                # Write the rows to the merge file
                for row_idx, row_dict in enumerate(csv_dict_reader):
                    processed_row_dict, error = process_data_row_dict(
                        row_dict, schema_dict["fields"], season, path
                    )

                    if error is None:
                        csv_dict_writer.writerow(processed_row_dict)

                    else:
                        if error == "Invalid number of columns":
                            print("  Invalid number of columns on row:", row_idx + 3)

                            invalid_row_path = (
                                f"{merged_dir_path}/invalid_num_cols_{schema}.csv"
                            )

                            # Write the row to file
                            with open(invalid_row_path, "a", newline="") as file:
                                csv_writer = csv.writer(file)

                                # Write the header if the file is new
                                if file.tell() == 0:
                                    csv_writer.writerow(schema_dict["fields"])

                                row = []
                                for field in schema_dict["fields"]:
                                    if field in ["季度", "縣市", "類別"]:
                                        row.append(row_dict[field])

                                    else:
                                        row += (
                                            current_file[row_idx + 2]
                                            .replace("\r\n", "")
                                            .split(",")
                                        )
                                        csv_writer.writerow(row)
                                        break

                        elif error == "Invalid date":
                            print("  Invalid date on row:", row_idx + 3)

                            invalid_row_path = (
                                f"{merged_dir_path}/invalid_date_{schema}.csv"
                            )

                            with open(invalid_row_path, "a", newline="") as file:
                                csv_writer = csv.DictWriter(
                                    file, fieldnames=schema_dict["fields"]
                                )

                                if file.tell() == 0:
                                    csv_writer.writeheader()

                                csv_writer.writerow(processed_row_dict)

                        print(f"   Saved in {invalid_row_path}")

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


def process_date(date_str):
    if date_str is None:
        return None

    elif date_str == "":
        return ""

    # Pattern: 990101 or 1000101
    elif date_str.isdigit() and len(date_str) in [6, 7]:
        day = date_str[-2:]
        month = date_str[-4:-2]
        year = int(date_str[:-4])
        return f"{int(year)+1911}-{month}-{day}"

    # Pattern: 99年1月1日 or 100年1月1日
    elif match := re.findall(r"(\d+)年(\d+)月(\d+)日", date_str):
        year, month, day = match[0]
        return f"{int(year)+1911}-{month}-{day}"

    else:
        return "Invalid date"


def process_data_row_dict(row_dict, fields, season, raw_file_path):
    error = None

    for field in fields:
        if field == "季度":
            row_dict[field] = season.replace("S", "Q")

        elif field == "縣市":
            code = raw_file_path.split("/")[-1][0]
            row_dict[field] = config["code_mappings"]["city"][code]

        elif field == "類別":
            code = raw_file_path.split("_")[-2]
            row_dict[field] = config["code_mappings"]["category"][code]

        elif field in [
            "交易年月日",
            "建築完成年月",
            "建築完成日期",
            "租賃年月日",
        ]:
            processed_date = process_date(row_dict[field])

            if processed_date == "Invalid date":
                error = "Invalid date"

            else:
                row_dict[field] = processed_date

        elif field == "租賃期間-起" and "租賃期間" in row_dict:
            row_dict[field] = row_dict["租賃期間"].split("~")[0]

        elif field == "租賃期間-迄" and "租賃期間" in row_dict:
            row_dict[field] = row_dict["租賃期間"].split("~")[-1]
            del row_dict["租賃期間"]

        elif (
            field == "車位移轉總面積平方公尺" and "車位移轉總面積(平方公尺)" in row_dict
        ):
            row_dict[field] = row_dict.pop("車位移轉總面積(平方公尺)")

        elif field == "土地移轉面積平方公尺" and "土地移轉面積(平方公尺)" in row_dict:
            row_dict[field] = row_dict.pop("土地移轉面積(平方公尺)")

        elif field not in row_dict:
            row_dict[field] = ""

    if len(row_dict) != len(fields):
        # print(list(row_dict.keys()))
        # print(fields)
        error = "Invalid number of columns"

    return row_dict, error


def process_invalid_date(path="data/merged/invalid_date_*.csv"):
    paths = glob.glob(path)

    for path in paths:
        schema = path.split(".")[0].split("_")[-1]
        fields = config["schemas"][schema]["fields"]

        row_dicts = []
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for field in fields:
                    if field in [
                        "交易年月日",
                        "建築完成年月",
                        "建築完成日期",
                        "租賃年月日",
                        "租賃期間-起",
                        "租賃期間-迄",
                    ]:
                        if not re.match(r"\d{4}-\d{2}-\d{2}", row[field]):
                            row[field] = ""
                row_dicts.append(row)

        with open(
            "/".join(path.split("/")[:-1]) + f"/corrected_date_{schema}.csv", "w"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(row_dicts)

        print("Processed", path)
        os.remove(path)
        print("Removed", path)


if __name__ == "__main__":
    # lvr.save_history_season_raw_data()
    lvr.save_season_raw_data()
    merge_csv_all_schemas()
    process_invalid_date()
