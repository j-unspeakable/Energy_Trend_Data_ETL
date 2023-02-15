import os
import time
import logging
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not os.path.exists('./logs'):
    os.makedirs('./logs')

file_handler = logging.FileHandler('./logs/ingest_data.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def ingest_excel_files(url, html_name):
    try:
        # Get the webpage HTML.
        response = requests.get(url)

        # Parse the HTML using BeautifulSoup.
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all links that end with .xls or .xlsx
        file_links = [(link.get('href'), link.text) for link in soup.find_all('a') if link.get('href') and link.get('href').endswith(('.xls', '.xlsx'))]

        # Find the link to the Excel file with the HTML name on the site.
        target_link = None
        for link, text in file_links:
            response = requests.get(link)
            if response.ok and html_name in text:
                target_link = link
                break

        # Check if the file already exists and if it is newer on the website.
        filename = os.path.basename(target_link)
        if target_link is not None:
            if os.path.exists(f"./data/{filename}"):
                local_mod_time = os.path.getmtime(f"./data/{filename}")
                response = requests.head(target_link)
                website_mod_time = time.mktime(time.strptime(response.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z'))
                if website_mod_time <= local_mod_time:
                    logger.info(f'{filename} already up-to-date')
                else:
                    # Download the target Excel file.
                    response = requests.get(target_link)
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    logger.info(f'{filename} updated successfully')
            else:
                if not os.path.exists('./data'):
                    os.makedirs('./data')
                response = requests.get(target_link)
                with open(f"./data/{filename}", 'wb') as f:
                    f.write(response.content)
                logger.info(f'{filename} downloaded successfully')
        else:
            logger.info('Excel file not found')

    except requests.exceptions.ConnectionError as e:
        logger.error(e)

    return filename