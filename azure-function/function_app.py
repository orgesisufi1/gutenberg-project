import azure.functions as func
import logging
import requests
import pyodbc
import time
from datetime import datetime
import json
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
import azure.functions as azfunc
from dotenv import load_dotenv
import os

load_dotenv()

app = func.FunctionApp()

BLOB_CONNECTION_STRING = os.getenv('BLOB_CONNECTION_STRING')
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

SQL_CONNECTION_STRING = os.getenv('SQL_CONNECTION_STRING')

BASE_URL = os.getenv('BASE_URL')

DATABRICKS_INSTANCE = os.getenv('DATABRICKS_INSTANCE')
DATABRICKS_TOKEN =os.getenv('DATABRICKS_TOKEN')
JOB_ID = os.getenv('JOB_ID')

TIMEOUT_LIMIT = 9 * 60

@app.function_name(name="TimerTriggerFetchBooks")
@app.schedule(schedule="0 0 */12 * * *", arg_name="mytimer", run_on_startup=True, use_monitor=True)
def timer_trigger_function(mytimer: func.TimerRequest) -> None:
    logging.info("Timer function triggered.")
    start_time = time.time()

    def get_last_url():
        try:
            conn = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = conn.cursor()
            query = "SELECT TOP 1 LastUrl, LastBookID FROM LastProcessedFile ORDER BY LastUpdated DESC"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            if result:
                return result[0], result[1]
            return None, None
        except Exception as e:
            logging.error(f"Error fetching last processed URL: {str(e)}")
            return None, None

    def save_last_url(url, last_book_id):
        try:
            conn = pyodbc.connect(SQL_CONNECTION_STRING)
            cursor = conn.cursor()
            current_datetime = datetime.now()
            insert_query = "INSERT INTO LastProcessedFile (LastUrl, LastBookID, LastUpdated) VALUES (?, ?, ?)"
            cursor.execute(insert_query, (url, last_book_id, current_datetime))
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"Last processed URL and book ID stored: {url}, {last_book_id}")
        except Exception as e:
            logging.error(f"Error storing last processed URL: {str(e)}")

    def fetch_books():
        last_url, last_book_id = get_last_url()
        url = last_url if last_url else BASE_URL
        last_processed_book = last_book_id if last_book_id else 0
        logging.info(f'URL AND BOOK_ID: {url}, {last_processed_book}')

        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

        current_date = datetime.now().strftime("%Y-%m-%d")

        while url:
            logging.info(f'Processing page: {url}')

            if time.time() - start_time >= TIMEOUT_LIMIT - 90:
                logging.warning("Approaching timeout, saving progress and exiting.")
                save_last_url(url, last_processed_book)

                databricks_url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"

                databricks_headers = {
                    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                    "Content-Type": "application/json"
                }

                databricks_data = {
                    "job_id": JOB_ID
                }

                response = requests.post(databricks_url, headers=databricks_headers, json=databricks_data)

                logging.info("DATABRICKS JOB STARTED!!")
                break

            response = requests.get(url)
            if response.status_code != 200:
                logging.error(f"Error fetching data: {response.status_code}")
                break
            elif not response.text.strip():
                logging.error(f"Received empty response from {url}")
                break
            else:
                try:
                    data = response.json()
                except ValueError as e:
                    logging.error(f"Error decoding JSON from the response: {e}")
                    break


            books = data.get("results", [])

            for book in books:
                book_id = book.get('id')
                if not book_id:
                    logging.warning("Skipping book due to missing ID")
                    continue

                if book_id <= last_processed_book:
                    logging.info(f"Skipping already processed book ID: {book_id}")
                    continue

                book_title = book['title'].translate(str.maketrans('', '', "#%&{}\\<>*?/!'\":@+`|=\n\t;,.-[]()$"))
                if not book_title:
                    logging.info("Skipping book due to missing title")
                    continue

                book_author = book['authors'][0]['name'] if book.get('authors') else "Unknown"

                category = book['bookshelves']
                cleaned_data = [item.replace("Browsing: ", "") for item in category]

                book_link = book['formats'].get('text/plain; charset=us-ascii')
                if not book_link:
                    logging.warning(f"Skipping book {book_title} due to missing text link")
                    continue

                gutenberg_url = f"https://www.gutenberg.org/ebooks/{book_id}"
                try:
                    response = requests.get(gutenberg_url)
                    response.raise_for_status() 
                    soup = BeautifulSoup(response.text, "html.parser")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch metadata page for book {book_id}: {str(e)}")
                    continue

                metadata_section = soup.find("table", class_="bibrec")
                if not metadata_section:
                    logging.warning(f"Metadata section not found for book ID {book_id}")
                    continue

                release_date = None
                for row in metadata_section.find_all("tr"):
                    header = row.find("th")
                    value = row.find("td")
                    if header and value and "Release Date" in header.text:
                        release_date = value.text.strip()
                        break
                if not release_date:
                    logging.warning(f"Release date not found for book ID {book_id}")

                try:
                    book_text = requests.get(book_link).text
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch book text for book ID {book_id}: {str(e)}")
                    continue

                # Save book data in a JSON object
                schema = {
                    'book_id': book_id,
                    'book_title': book_title,
                    'book_author': book_author,
                    'book_category': cleaned_data,
                    'release_date': release_date,
                    'book_text': book_text
                }

                # Generate blob name
                blob_name = f'{current_date}/{book_id}_{book_title}.json'.translate(str.maketrans('', '', "#%&{}\\<>*? $!'\":@+`|=\n\t;,"))
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(json.dumps(schema, indent=4), overwrite=True)
                logging.info(f"Uploaded {blob_name} to Blob Storage")

                last_processed_book = book_id


            
            save_last_url(url, last_processed_book)
            logging.info(f'SAVING {url} AND {last_processed_book}')

            url = data.get("next")
            
            if not url:
                logging.info("No more pages to fetch, stopping.")
                break

    fetch_books()
    logging.info("Books stored successfully in Azure Blob Storage")

