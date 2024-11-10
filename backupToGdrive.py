import os
import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import asyncio

# Generate log filename with the current date and time
log_filename = f"C:\\Path\\To\\Log\\folder\\backup-upload-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"

print(f"The log file will be generated at: {os.path.abspath(log_filename)}")

# Logging configuration to save in a unique file per execution
logging.basicConfig(
    filename=log_filename,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_latest_file(folder_path):
    """Gets the latest file in a folder."""
    try:
        files = [os.path.join(folder_path, file) for file in os.listdir(folder_path)]
        latest_file = max(files, key=os.path.getmtime)
        logging.info(f"Latest file: {latest_file}")
        return latest_file
    except Exception as e:
        logging.error(f"Error getting the latest file: {e}")
        raise

def delete_zip_files(credentials_path):
    """Deletes all .zip files in the Google Drive account linked to the service account."""
    try:
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        page_token = None
        while True:
            # List all files and filter for .zip files
            response = service.files().list(
                q="name contains '.zip'",  # Filter for .zip files
                pageSize=100,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token
            ).execute()
            
            files = response.get('files', [])
            if not files:
                logging.info("No .zip files found to delete.")
                return

            for file in files:
                # Delete file asynchronously
                asyncio.create_task(delete_file_async(service, file['id'], file['name']))

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    except HttpError as error:
        logging.error(f"HTTP error while trying to delete .zip files: {error}")
    except Exception as e:
        logging.error(f"General error while trying to delete .zip files: {e}")

async def delete_file_async(service, file_id, file_name):
    """Asynchronous function to delete a specific file."""
    try:
        service.files().delete(fileId=file_id).execute()
        logging.info(f".zip file deleted: {file_name}")
    except HttpError as error:
        logging.error(f"HTTP error deleting the file {file_name}: {error}")
    except Exception as e:
        logging.error(f"General error deleting the file {file_name}: {e}")

def upload_and_share_file(credentials_path, file_path, user_email):
    """Uploads a file to Google Drive and shares it with a specific user."""
    try:
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)

        # File metadata for Google Drive
        file_metadata = {
            'name': os.path.basename(file_path)
        }
        media = MediaFileUpload(file_path, resumable=True)

        logging.info(f"Uploading file: {file_path}")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()

        logging.info(f"File uploaded successfully. File ID: {file.get('id')}")

        # Share the uploaded file with the specified user
        share_file_with_user(service, file.get('id'), user_email)

    except HttpError as error:
        logging.error(f"HTTP error uploading and sharing the file: {error}")
    except Exception as e:
        logging.error(f"General error uploading and sharing the file: {e}")

def share_file_with_user(service, file_id, user_email):
    """Shares the file with a specific user."""
    try:
        user_permission = {
            'type': 'user',
            'role': 'reader',  # Change to 'writer' if you need edit permissions
            'emailAddress': user_email
        }
        service.permissions().create(
            fileId=file_id,
            body=user_permission,
            fields='id'
        ).execute()
        logging.info(f"File shared with {user_email}")
    except Exception as e:
        logging.error(f"Error sharing the file: {e}")

if __name__ == "__main__":
    folder_path = r"C:\Path\To\Backup\Folder"
    credentials_path = r"C:\Path\To\Credentials\client.json"
    user_email = "your-email@example.com"
    try:
        # First, delete existing .zip files
        delete_zip_files(credentials_path)

        # Get the latest file in the specified folder
        latest_file = get_latest_file(folder_path)
        logging.info(f"Uploading the latest file: {latest_file}")

        # Upload and share the latest file
        upload_and_share_file(credentials_path, latest_file, user_email)
    except Exception as e:
        logging.error(f"Error in the main script: {e}")
