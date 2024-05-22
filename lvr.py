from datetime import date
import requests
import zipfile
import os


today = date.today()

if today.month in [1, 2, 3]:
    newest_season = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    newest_season = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    newest_season = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    newest_season = f"{today.year - 1911}S3"


def save_season_data(season=newest_season):
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


def save_history_season_data(start="101S1", end=newest_season):
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
