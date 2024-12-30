from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
import shutil
import requests
from PIL import Image
import numpy as np
import pyheif
import magic

# Constants
SERVICE_ACCOUNT_FILE = 'test-project-sheets-api.json' # from service account in google console
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly'] # for sheets API
SPREADSHEET_ID = '174u7UGhGkeXerdwF__146Np2R_P2b21lJKXOgAnj-08'
# 16VhHEhwux2cdAq_EIaXeSB9QU5pc_nlB8XDs6w0g3Eo (copied)
# 174u7UGhGkeXerdwF__146Np2R_P2b21lJKXOgAnj-08 (actual)
RANGE_NAME = 'Form Responses 1!A2:C' # {A: Timestamp, B: Image Link, C: Stave Count}

SAVE_FOLDER = 'data/raw_images'
# if save folder exists, delete it
if os.path.exists(SAVE_FOLDER):
    shutil.rmtree(SAVE_FOLDER)
# create save folder
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Authenticate and connect to sheets API
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)

# Fetch data from sheet
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
values = result.get('values', [])
if not values:
    print("[ERROR] No data found in the sheet.")
else:
    print(f"Fetched {len(values)} rows from Google Sheet.")

# extracting download url from google drive link
def get_direct_download_url(link):
    if "drive.google.com/open?id=" in link:
        file_id = link.split("id=")[-1]
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return None

# resize images via interpolation
def resize_image(image_array, target_size=(1024, 1024)):
    pil_img = Image.fromarray(image_array)
    resized_img = pil_img.resize(target_size, Image.BICUBIC)
    return np.array(resized_img)

image_data = []
labels = []

for row in values:
    if len(row) < 3:
        print(f"Skipping row with incomplete data: {row}")
        continue

    timestamp, link, label = row[0], row[1], row[2]
    timestamp = timestamp.replace("/", "-").replace(":", "-").replace(" ", "_") # ensures valid file name

    try:

        # download the image from the link
        download_url = get_direct_download_url(link)
        if not download_url:
            print(f"[INVALID LINK]: {link}")
        response = requests.get(download_url, stream=True)
        response.raise_for_status() # raise for HTTP error        

        # save image locally
        image_name = os.path.join(SAVE_FOLDER, f"raw_image_{timestamp}")
        with open(image_name, 'wb') as img_file:
            for chunk in response.iter_content(1024): # load a mb at a time in chunks
                img_file.write(chunk)
        print(f"Downloaded: {image_name}")

        # Detect file type using magic
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(image_name)

        # Determine file extension based on MIME type
        if 'heic' in file_type or 'heif' in file_type:
            file_extension = 'heic'
        elif 'jpeg' in file_type:
            file_extension = 'jpg'
        else:
            print(f"[ERROR] Unsupported file type detected: {file_type}")
            os.remove(image_name)  # Remove the unsupported file
            continue

        # Rename the file with the correct extension
        image_name_with_ext = f"{image_name}.{file_extension}"
        os.rename(image_name, image_name_with_ext)
        print(f"Renamed to: {image_name_with_ext}")

        # Convert HEIC files to RGB arrays
        if file_extension == 'heic':
            try:
                heif_file = pyheif.read(image_name_with_ext)  # Read HEIC file
                img = Image.frombytes(
                    heif_file.mode,
                    heif_file.size,
                    heif_file.data,
                    "raw",
                    heif_file.mode,
                    heif_file.stride,
                )
                img = img.convert('RGB')  # Ensure RGB format
                img_array = np.array(img)  # Convert to NumPy array
                img_array = resize_image(img_array, target_size=(1024, 1024)) # resize image
            except Exception as e:
                print(f"[ERROR] Failed to convert HEIC to array: {e}")
                continue
        else:
            # Convert JPG files to RGB arrays
            with Image.open(image_name_with_ext) as img:
                img = img.convert('RGB')
                img_array = np.array(img)
                img_array = resize_image(img_array, target_size=(1024, 1024)) # resize image

        # Store in lists
        image_data.append(img_array)
        labels.append(label)


    except Exception as e:
        print(f"[ERROR] Failed to download or process image from {link}: {e}")


print(f"Downloaded and processed {len(image_data)} images.")
# debugging
# print('image_data:', image_data)
# print('labels:', labels)
np.save('data/raw_image_arr.npy', image_data)
np.save('data/raw_labels_arr', labels)
print("Images and Labels saved as .npy files")
