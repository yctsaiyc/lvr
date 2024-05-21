from datetime import date
import requests
import zipfile
import os


# today = date(2012, 4, 1)  # Oldest data: 101S1
today = date.today()

if today.month in [1, 2, 3]:
    season = f"{today.year - 1912}S4"
elif today.month in [4, 5, 6]:
    season = f"{today.year - 1911}S1"
elif today.month in [7, 8, 9]:
    season = f"{today.year - 1911}S2"
else:  # today.month in [10, 11, 12]
    season = f"{today.year - 1911}S3"

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
