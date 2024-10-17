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
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_lvr_land:
    def __init__(self, config_path):
        self.config = self.get_config(config_path)
        self.newest_season = self.get_newest_season()
        self.raw_data_dir_path = self.config["raw_data_dir_path"]
        self.processed_data_dir_path = self.config["processed_data_dir_path"]
        self.prefix = "lvr_land"

    def get_config(self, config_path):
        with open(config_path) as file:
            return json.load(file)

    def get_newest_season(self):
        today = date.today()

        if today.month in [1, 2, 3]:
            return f"{today.year - 1912}S4"
        elif today.month in [4, 5, 6]:
            return f"{today.year - 1911}S1"
        elif today.month in [7, 8, 9]:
            return f"{today.year - 1911}S2"
        else:  # today.month in [10, 11, 12]
            return f"{today.year - 1911}S3"

    def save_season_raw_data(self, season=None):
        if season is None:
            season = self.newest_season

        url = self.config["url"] + season
        print("url:", url)

        response = requests.get(url)

        if response.status_code == 200:
            os.makedirs(self.raw_data_dir_path, exist_ok=True)

            # Save the zip file
            zip_path = os.path.join(self.raw_data_dir_path, f"lvr_landcsv_{season}.zip")

            with open(zip_path, "wb") as file:
                file.write(response.content)

            print(f"Downloaded:", zip_path)

            # Unzip
            data_package_path = zip_path.replace(".zip", "")

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(data_package_path)

            print(f"Unzipped:", data_package_path)

            # Delete the zip file
            os.remove(zip_path)
            print(f"Deleted:", zip_path)

        else:
            print("Failed to download the file. Status code:", response.status_code)

    def save_history_season_raw_data(self, start="101S1", end=None):
        if end is None:
            end = self.newest_season

        while int(start[-1]) <= 4:
            print(start)
            self.save_season_raw_data(start)
            start = start[:-1] + str(int(start[-1]) + 1)

        start = str(int(start[:3]) + 1) + "S1"

        for year in range(int(start[:3]), int(end[:3]) + 1):
            for season in range(1, 5):
                if f"{year}S{season}" > end:
                    break

                print(f"{year}S{season}")
                self.save_season_raw_data(f"{year}S{season}")

    def merge_csv(self, schema="main", season=None):
        if season is None:
            season = self.newest_season

        raw_data_dir_path = os.path.join(
            self.raw_data_dir_path, f"lvr_landcsv_{season}"
        )
        merged_dir_path = os.path.join(self.processed_data_dir_path, schema)
        os.makedirs(merged_dir_path, exist_ok=True)
        schema_dict = self.config["schemas"][schema]

        raw_file_paths = []
        for schema_file in schema_dict["files"]:
            raw_file_paths += glob.glob(
                os.path.join(raw_data_dir_path, schema_file["pattern"])
            )

        season = raw_data_dir_path[-5:]  # yyySs
        merged_file_path = os.path.join(
            merged_dir_path, f"{self.prefix}_{season}_{schema}.csv"
        )
        print(f"Merge {merged_file_path}...")

        df = pd.DataFrame(columns=schema_dict["fields"])

        # Create the merge file
        for path in raw_file_paths:
            print("", path)
            df = pd.concat([df, pd.read_csv(path, skiprows=[1])], ignore_index=True)

        df["原始資料"] = ""
        df.to_csv("tmp.csv", index=False)
        print("Saved: tmp.csv")
        df = self.process_df(df, season, "tmp.csv")
        df.to_csv("processed.csv", index=False)
        print("Saved: processed.csv")
        exit()

        # # 1. 若無錯誤則直接寫入
        # if error is None:
        #     csv_dict_writer.writerow(processed_row_dict)
        #
        # else:
        #     # 2.欄位數錯誤
        #     if error == "Invalid number of columns":
        #         print(
        #             "  Invalid number of columns on row:", row_idx + 3
        #         )
        #
        #         invalid_row_path = (
        #             f"{merged_dir_path}/invalid_num_cols_{schema}.csv"
        #         )
        #
        #         # Write the row to file
        #         with open(invalid_row_path, "a", newline="") as file:
        #             csv_writer = csv.writer(file)
        #
        #             # Write the header if the file is new
        #             if file.tell() == 0:
        #                 csv_writer.writerow(schema_dict["fields"])
        #
        #             row = []
        #             for field in schema_dict["fields"]:
        #                 if field in ["季度", "縣市", "類別"]:
        #                     row.append(row_dict[field])
        #
        #                 else:
        #                     row += (
        #                         current_file[row_idx + 2]
        #                         .replace("\r\n", "")
        #                         .split(",")
        #                     )
        #                     csv_writer.writerow(row)
        #                     break
        #
        #     # 3. 日期錯誤
        #     elif error == "Invalid date":
        #         print("  Invalid date on row:", row_idx + 3)
        #
        #         invalid_row_path = (
        #             f"{merged_dir_path}/invalid_date_{schema}.csv"
        #         )
        #
        #         with open(invalid_row_path, "a", newline="") as file:
        #             csv_writer = csv.DictWriter(
        #                 file, fieldnames=schema_dict["fields"]
        #             )
        #
        #             if file.tell() == 0:
        #                 csv_writer.writeheader()
        #
        #             csv_writer.writerow(processed_row_dict)
        #
        #     # 4. 特殊字元
        #     elif error == "Dirty char":
        #         print("  Dirty char on row:", row_idx + 3)
        #
        #         invalid_row_path = (
        #             f"{merged_dir_path}/dirty_char_{schema}.csv"
        #         )
        #
        #         with open(invalid_row_path, "a", newline="") as file:
        #             csv_writer = csv.DictWriter(
        #                 file, fieldnames=schema_dict["fields"]
        #             )
        #
        #             if file.tell() == 0:
        #                 csv_writer.writeheader()
        #
        #             csv_writer.writerow(processed_row_dict)
        #
        #     print(f"   Saved in {invalid_row_path}")

        print("Merge done!\n")

    def merge_csv_all_schemas(self, season="???S?"):
        raw_dir_paths = glob.glob(
            os.path.join(self.raw_data_dir_path, f"lvr_landcsv_{season}")
        )

        for raw_dir_path in raw_dir_paths:
            season = raw_dir_path[-5:]
            print(f"Season: {season}\n")

            for schema in self.config["schemas"]:
                self.merge_csv(schema, season)

    def process_date(self, date_str):
        if date_str is None:
            return None

        elif date_str == "":
            return ""

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
            # Try to create a datetime object from the string
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            # If a ValueError is raised, the date is invalid
            return "Invalid date"

    def process_special_chars(self, df):
        special_chars = ['"', "'", "\\"]

        for idx, row in df.iterrows():
            for col in df.columns:
                if isinstance(row[col], str):
                    for char in special_chars:
                        if char in row[col]:
                            df.at[idx, col] = row[col].replace(char, "")
                            df.at[idx, "原始資料"] += f"{col}：{row[col]}。"

        return df

    def fill_info(self, df, season, raw_file_path):
        df["季度"] = season.replace("S", "Q")

        code = raw_file_path.split("/")[-1][0]
        df["縣市"] = self.config["code_mappings"]["city"][code]

        if "類別" in df.columns:
            code = raw_file_path.split("_")[-2]
            df["類別"] = self.config["code_mappings"]["category"][code]

        return df

    def process_df(self, df, season, raw_file_path):
        # 1. 檢查特殊字元
        df = self.process_special_chars(df)

        # 2. 填入季度、縣市、類別
        df = self.fill_info(df, season, raw_file_path)

        return df

        # for field in fields:
        #     # 3. 處理日期
        #     elif field in [
        #         "交易年月日",
        #         "建築完成年月",
        #         "建築完成日期",
        #         "租賃年月日",
        #     ]:
        #         processed_date = self.process_date(row_dict[field])

        #         if processed_date == "Invalid date":
        #             error = "Invalid date"

        #         else:
        #             row_dict[field] = processed_date

        #     # 4. 分離租賃期間
        #     elif field == "租賃期間-起" and "租賃期間" in row_dict:
        #         row_dict[field] = self.process_date(row_dict["租賃期間"].split("~")[0])

        #     elif field == "租賃期間-迄" and "租賃期間" in row_dict:
        #         row_dict[field] = self.process_date(row_dict["租賃期間"].split("~")[-1])
        #         del row_dict["租賃期間"]

        #     # 5. 處理欄位名稱
        #     elif (
        #         field == "車位移轉總面積平方公尺"
        #         and "車位移轉總面積(平方公尺)" in row_dict
        #     ):
        #         row_dict[field] = row_dict.pop("車位移轉總面積(平方公尺)")

        #     elif (
        #         field == "土地移轉面積平方公尺" and "土地移轉面積(平方公尺)" in row_dict
        #     ):
        #         row_dict[field] = row_dict.pop("土地移轉面積(平方公尺)")

        #     # 6. 若欄位不存在，填入空字串
        #     elif field not in row_dict:
        #         row_dict[field] = ""

        # # 7. 檢查欄位數 (若值有","，會被視為欄位分離符號)
        # if len(row_dict) != len(fields):
        #     error = "Invalid number of columns"

        # return row_dict, error

    def process_invalid_date(self):
        paths = glob.glob(
            f"{self.config['processed_data_dir_path']}/*/invalid_date_*.csv"
        )

        for path in paths:
            schema = path.replace(".csv", "").split("_")[-1]
            fields = self.config["schemas"][schema]["fields"]

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
                            try:
                                datetime.strptime(row[field], "%Y-%m-%d")
                            except ValueError:
                                row[field] = ""

                    row_dicts.append(row)

            with open(
                "/".join(path.split("/")[:-1]) + f"/rm_invalid_date_{schema}.csv", "w"
            ) as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                writer.writerows(row_dicts)

            print("Processed", path)

    def process_main(self, dir_path):
        paths = glob.glob(f"{dir_path}/*.csv")

        for path in paths:
            df = pd.read_csv(path)

            df["土地移轉總面積坪"] = df["土地移轉總面積平方公尺"] * 0.3025
            df["建物移轉總面積坪"] = df["建物移轉總面積平方公尺"] * 0.3025
            df["車位移轉總面積坪"] = df["車位移轉總面積平方公尺"] * 0.3025
            df["單價元坪"] = df["單價元平方公尺"] * 0.3025

            df = df.drop(
                columns=[
                    "土地移轉總面積平方公尺",
                    "建物移轉總面積平方公尺",
                    "車位移轉總面積平方公尺",
                    "單價元平方公尺",
                ]
            )
            df.to_csv(path, index=False)

    def crawling(self):
        try:
            # 1. 存原始資料csv
            # self.save_season_raw_data()

            # 2. 將不同縣市資料依schema合併
            self.merge_csv_all_schemas()

            # self.process_invalid_date()
            # self.process_dirty_char()
            # for file_to_remove in glob.glob(
            #     f"{self.config['processed_data_dir_path']}/*/invalid_date_*.csv"
            # ):
            #     os.remove(file_to_remove)
            #     print("Removed", file_to_remove)
            # for file_to_remove in glob.glob(
            #     f"{self.config['processed_data_dir_path']}/*/dirty_char_*.csv"
            # ):
            #     os.remove(file_to_remove)
            #     print("Removed", file_to_remove)
            # shutil.rmtree(self.config["raw_data_dir_path"])

            # self.process_main(
            #     os.path.join(self.config["processed_data_dir_path"], "main")
            # )

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_lvr_land = ETL_lvr_land("lvr_land.json")
    etl_lvr_land.crawling()
