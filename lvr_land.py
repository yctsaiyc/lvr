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

    # 下載單一季度原始資料
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

    # 下載所有季度原始資料
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

    # 依schema合併資料
    def merge_csv(self, schema="main", season=None):
        if season is None:
            season = self.newest_season

        # 該季度原始資料位置
        raw_data_dir_path = os.path.join(
            self.raw_data_dir_path, f"lvr_landcsv_{season}"
        )

        # 合併後資料位置
        merged_dir_path = os.path.join(self.processed_data_dir_path, schema)
        os.makedirs(merged_dir_path, exist_ok=True)

        # schema資訊（檔名pattern、欄位）
        schema_dict = self.config["schemas"][schema]

        # 符合pattern的檔案路徑
        raw_file_paths = []
        for schema_file in schema_dict["files"]:
            raw_file_paths += glob.glob(
                os.path.join(raw_data_dir_path, schema_file["pattern"])
            )

        # 合併後資料檔名、路徑
        merged_file_path = os.path.join(
            merged_dir_path, f"{self.prefix}_{season}_{schema}.csv"
        )
        print(f"Merge {merged_file_path}...")

        # 依config定義的schema建立DataFrame
        df = pd.DataFrame(columns=schema_dict["fields"])

        # 所有符合pattern的檔案
        for path in raw_file_paths:
            print("", path)

            # 讀檔案，跳過第二行（英文header）
            df2 = pd.read_csv(path, skiprows=[1], dtype=str)

            # 合併
            df = pd.concat([df, df2], ignore_index=True)

        # 新增欄位存被處理過資料的原始值
        df["原始資料"] = ""

        # 處理資料
        df = self.process_df(df, season, f"tmp_{schema}.csv")

        # 存檔
        df.to_csv(merged_file_path, index=False)
        print("Saved:", merged_file_path)

    # 依schema合併資料（一次處理所有schema）
    def merge_csv_all_schemas(self, season="???S?"):
        raw_dir_paths = glob.glob(
            os.path.join(self.raw_data_dir_path, f"lvr_landcsv_{season}")
        )

        # 季度
        for raw_dir_path in raw_dir_paths:
            season = raw_dir_path[-5:]
            print(f"Season: {season}\n")

            # schema
            for schema in self.config["schemas"]:
                self.merge_csv(schema, season)

    # 處理日期格式
    def process_date(self, df):
        # 轉成yyyy-mm-dd
        def convert_date(date_str):
            # 空值直接回傳
            if pd.isna(date_str) or date_str == "":
                return ""

            match = re.findall(r"(\d+)年(\d+)月(\d+)日", date_str)

            # Pattern: 990101 or 1000101
            if date_str.isdigit() and len(date_str) in [6, 7]:
                day = date_str[-2:]
                month = date_str[-4:-2]
                year = int(date_str[:-4])
                return f"{int(year)+1911}-{month}-{day}"

            # Pattern: 99年1月1日 or 100年1月1日
            elif match:
                year, month, day = match[0]
                return f"{int(year)+1911}-{month}-{day}"

            # 非空值且不符pattern者直接回傳
            return date_str

        def validate_date(row, date_col):
            date_str = row[date_col]

            try:
                # 確認真的有這一天（反例：2022-02-29、2023-09-31、2024-10-00）
                if date_str:
                    datetime.strptime(date_str, "%Y-%m-%d")
                    return date_str

            except ValueError:
                # 若沒有這一天，則取代為空值，並記錄原始資料
                original_data = row.get("原始資料", "")
                updated_data = f"{original_data}{date_col}：{date_str}。"
                df.at[row.name, "原始資料"] = updated_data
                return ""

        date_cols = [
            "交易年月日",
            "建築完成年月",
            "建築完成日期",
            "租賃年月日",
            "租賃期間-起",
            "租賃期間-迄",
        ]

        for date_col in date_cols:
            if date_col not in df.columns:
                continue

            df[date_col] = df[date_col].apply(convert_date)
            df[date_col] = df.apply(lambda row: validate_date(row, date_col), axis=1)

        return df

    # 處理特殊字元
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

    # 填入季度、縣市、類別
    def fill_info(self, df, season, raw_file_path):
        df["季度"] = season.replace("S", "Q")

        code = raw_file_path.split("/")[-1][0]
        df["縣市"] = self.config["code_mappings"]["city"][code]

        if "類別" in df.columns:
            code = raw_file_path.split("_")[-2]
            df["類別"] = self.config["code_mappings"]["category"][code]

        return df

    # 平方公尺轉坪，取到小數第二位
    def m2_to_ping(self, df):
        for col in df.columns:
            if "平方公尺" in col:
                df[col] = (df[col].astype(float) * 0.3025).round(2)
                df.rename(columns={col: col.replace("平方公尺", "坪")}, inplace=True)

        return df

    def process_df(self, df, season, raw_file_path):
        # 1. 檢查特殊字元
        df = self.process_special_chars(df)

        # 2. 填入季度、縣市、類別
        df = self.fill_info(df, season, raw_file_path)

        # 3. 分離租賃期間
        if "租賃期間" in df.columns:
            df["租賃期間-起"] = df["租賃期間"].str.split("~").str[0]
            df["租賃期間-迄"] = df["租賃期間"].str.split("~").str[-1]
            df = df.drop(columns=["租賃期間"])

        # 4. 處理日期
        df = self.process_date(df)

        # 5. 平方公尺轉坪
        df = self.m2_to_ping(df)
        return df

        # 6. 處理欄位名稱
        if "車位移轉總面積平方公尺" in df.columns:
            df.rename(
                columns={"車位移轉總面積平方公尺": "車位移轉總面積(平方公尺)"},
                inplace=True,
            )

        if "土地移轉面積平方公尺" in df.columns:
            df.rename(
                columns={"土地移轉面積平方公尺": "土地移轉面積(平方公尺)"},
                inplace=True,
            )

    def crawling(self):
        try:
            # # 0. 存歷史資料
            # self.save_history_season_raw_data()

            # # 1. 存原始資料csv
            # self.save_season_raw_data()

            # 2. 將不同縣市資料依schema合併
            self.merge_csv_all_schemas(season="113S3")

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_lvr_land = ETL_lvr_land("lvr_land.json")
    etl_lvr_land.crawling()
