from io import BytesIO
from zipfile import ZipFile
import urllib.request
import glob
import os

def filelist(dest_dir = "/tmp/nasa"):
    return [os.path.join(dp, f) for dp, dn, filenames in os.walk(dest_dir) for f in filenames if os.path.splitext(f)[1] == '.csv']

def download(urlstr: str, dest_dir = "/tmp/nasa"):
    url = urllib.request.urlopen(urlstr)
    with ZipFile(BytesIO(url.read()), 'r') as zip_ref:
            zip_ref.extractall(dest_dir)
    return filelist(dest_dir)

    return os.listdir(dest_dir)

if __name__ == "__main__":
    url = "https://firms.modaps.eosdis.nasa.gov/data/country/zips/modis_2021_all_countries.zip"
    fl = download(url)
    print(fl)
