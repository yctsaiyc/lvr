from datetime import datetime, date
import requests
import zipfile
import os
import json
import glob
import csv
import re
import sys
import shutil


today = date.today()

if today.month in [1, 2, 3]:
    NEWEST_SEASON = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    NEWEST_SEASON = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    NEWEST_SEASON = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    NEWEST_SEASON = f"{today.year - 1911}S3"

config = {}


def set_config(config_path):
    with open(config_path, "r") as f:
        global config
        config = json.load(f)


def save_season_raw_data(season=NEWEST_SEASON):
    url = config["url"] + season
    print("url:", url)

    response = requests.get(url)

    if response.status_code == 200:
        dir_path = os.path.join(config["data_path"], "raw")
        os.makedirs(dir_path, exist_ok=True)

        # Write the content of the response to the file
        file_path = os.path.join(dir_path, f"lvr_landcsv_{season}")
        with open(f"{file_path}.zip", "wb") as file:
            file.write(response.content)
        print(f"Downloaded lvr_landcsv_{season}.zip")

        # Unzip
        with zipfile.ZipFile(f"{file_path}.zip", "r") as zip_ref:
            zip_ref.extractall(file_path)
        print(f"Unzipped lvr_landcsv_{season}")

        # Delete the zip file
        os.remove(f"{file_path}.zip")
        print(f"Deleted lvr_landcsv_{season}.zip\n")

    else:
        print("Failed to download the file. Status code:", response.status_code)


def save_history_season_raw_data(start="101S1", end=NEWEST_SEASON):
    while int(start[-1]) <= 4:
        print(start)
        save_season_raw_data(start)
        start = start[:-1] + str(int(start[-1]) + 1)
    start = str(int(start[:3]) + 1) + "S1"

    for year in range(int(start[:3]), int(end[:3]) + 1):
        for season in range(1, 5):
            if f"{year}S{season}" > end:
                break
            print(f"{year}S{season}")
            save_season_raw_data(f"{year}S{season}")


def organize_season_raw_data_paths(season=NEWEST_SEASON):
    print(f"Organizing {season} files...")
    dir_path = os.path.join(config["data_path"], "raw")

    for schema in config["schemas"].keys():
        for files in config["schemas"][schema]["files"]:
            src_path_pattern = os.path.join(
                dir_path, f"lvr_landcsv_{season}", files["pattern"]
            )
            src_paths = glob.glob(src_path_pattern)
            dest_dir = os.path.join(dir_path, schema)
            os.makedirs(dest_dir, exist_ok=True)

            for src_path in src_paths:
                city = os.path.basename(src_path).split("_")[0]
                category = (
                    os.path.basename(src_path)
                    .replace(".csv", "")
                    .split("lvr_land_")[-1][0]
                )
                dest_path = os.path.join(
                    dest_dir, f"{schema}_{season}_{city}_{category}.csv"
                )
                shutil.move(src_path, dest_path)

    shutil.rmtree(os.path.join(dir_path, f"lvr_landcsv_{season}"))
    print("Done!")


def organize_season_raw_data_paths_all_season():
    dir_paths = glob.glob(os.path.join(config["data_path"], "raw", "lvr_landcsv_???S?"))

    # Regular expression to match the pattern and capture the '???S?' part
    regex = re.compile(r"lvr_landcsv_(\w{3}S\w)")

    for dir_path in dir_paths:
        dir_path = os.path.basename(dir_path)
        season = regex.match(dir_path)
        if season:
            organize_season_raw_data_paths(season.group(1))


def merge_csv(schema="main", season=NEWEST_SEASON):
    raw_dir_path = f"{config['raw_data_path']}/lvr_landcsv_{season}"
    merged_dir_path = os.path.join(config["processed_data_path"], schema)
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
                    processed_row_dict = process_data_row_dict(
                        row_dict, schema, schema_dict["fields"], season, path
                    )

                    if processed_row_dict != "Invalid":
                        csv_dict_writer.writerow(processed_row_dict)

        print("Merge done!\n")


def merge_csv_all_schemas(season="???S?"):
    raw_dir_paths = glob.glob(f"{config['raw_data_path']}/lvr_landcsv_{season}")

    for raw_dir_path in raw_dir_paths:
        season = raw_dir_path[-5:]
        print(f"Season: {season}\n")
        for schema in config["schemas"]:
            merged_dir_path = os.path.join(config["processed_data_path"], schema)
            merge_csv(schema, season)


def process_date(date_str):
    if date_str in ("", None):
        return date_str

    match = re.findall(r"(\d+)年(\d+)月(\d+)日", date_str)

    # Pattern: 990101 or 1000101
    if date_str.isdigit() and len(date_str) in [6, 7]:
        day = date_str[-2:]
        month = date_str[-4:-2]
        year = int(date_str[:-4])
        date_str = f"{int(year)+1911}-{month}-{day}"

    # Pattern: 99年1月1日 or 100年1月1日
    elif match:
        year, month, day = match[0]
        date_str = f"{int(year)+1911}-{month}-{day}"

    try:
        return datetime.strptime(date_str, "%Y-%m-%d")

    except ValueError:
        return ""


def save_invalid_data(schema, row_dict):
    dir_path = os.path.join(config["data_path"], "invalid_data")
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"invalid_data_{schema}.csv")

    row = [row_dict[field] for field in row_dict.keys()]
    with open(file_path, "a") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def process_data_row_dict(row_dict, schema, fields, season, raw_file_path):
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
            row_dict[field] = process_date(row_dict[field])

        elif field == "租賃期間-起" and "租賃期間" in row_dict:
            row_dict[field] = process_date(row_dict["租賃期間"].split("~")[0])

        elif field == "租賃期間-迄" and "租賃期間" in row_dict:
            row_dict[field] = process_date(row_dict["租賃期間"].split("~")[-1])
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
        save_invalid_data(schema, row_dict)
        return "Invalid"

    return row_dict


def rm_special_char(csv_path):
    rows = []
    ctrl_chars = "".join(chr(i) for i in range(0, 32) if i != 10)
    trans_tb = str.maketrans("", "", ctrl_chars)

    with open(csv_path, mode="r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)

        for row in reader:
            # Remove control characters
            row = [cell.translate(trans_tb) for cell in row]

            # Replace "\," with ","
            row = [cell[:-1] if cell.endswith("\\") else cell for cell in row]
            rows.append(row)

    # Write the cleaned data back to the CSV file
    with open(csv_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(rows)


def is_valid_datatype(string, datatype):
    if string in ("", None):
        return True

    elif datatype == "string":
        return True

    elif datatype == "bool":
        if string in ("有", "無"):
            return True
        return False

    try:
        if datatype == "int":
            int(string)
            return True

        elif datatype == "float":
            float(string)
            return True

        elif datatype == "date":
            datetime.strptime(string, "%Y-%m-%d")
            return True

        return False

    except ValueError:
        return False


def check_datatype(file_path):
    print("Checking", file_path)
    schema = file_path.split(".")[0].split("_")[-1]
    field_dict = config["schemas"][schema]["fields"]

    with open(file_path) as f:
        reader = csv.DictReader(f)
        row_dicts = []

        for row_dict in reader:
            for field in field_dict:
                datatype = field_dict[field]
                if not is_valid_datatype(row_dict[field], datatype):
                    save_invalid_data(schema, row_dict)
                    break
            row_dicts.append(row_dict)

    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=field_dict.keys())
        writer.writeheader()
        writer.writerows(row_dicts)


def check_datatype_all():
    file_paths = glob.glob(f"{config['data_path']}/merged/*.csv")
    for file_path in file_paths:
        check_datatype(file_path)


def crawling(config_path):
    set_config(config_path)

    save_history_season_raw_data("112S4", "113S1")
    organize_season_raw_data_paths_all_season()
    return
    merge_csv_all_schemas()
    process_invalid_date()
    process_dirty_char()
    for file_to_remove in glob.glob(
        f"{config['processed_data_path']}/*/invalid_date_*.csv"
    ):
        os.remove(file_to_remove)
        print("Removed", file_to_remove)
    for file_to_remove in glob.glob(
        f"{config['processed_data_path']}/*/dirty_char_*.csv"
    ):
        os.remove(file_to_remove)
        print("Removed", file_to_remove)
    shutil.rmtree(config["raw_data_path"])
