from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests

from docx import Document
from io import BytesIO
import pandas as pd
import logging
import boto3
import fitz
import json
import re
import os

logging.basicConfig(
    filename='output.log',
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

# AWS credentials
# aws_access_key_id = ''
# aws_secret_access_key = ''
#
# s3 = boto3.client('s3', aws_access_key_id, aws_secret_access_key)  # Initialize the boto3 client
# bucket_name = 'gov-bids2'  # Specify your bucket name

file_extensions = ['.pdf', '.csv', '.docx', '.doc', '.zip', '.xlsx', '.png']

mime_to_extension = {
    'application/pdf': 'pdf',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'text/csv': 'csv',
    'application/csv': 'csv'
}

def sanitize_filename(url):
    """
    Sanitize the URL to be safe for use as a filename by removing disallowed characters and truncating if necessary.
    """

    cleaned_url = url.replace('http://', '').replace('https://', '') # Remove the protocol (http, https)
    sanitized = re.sub(r'[<>:"/\\|?*=]', '_', cleaned_url) # Replace slashes and other disallowed characters with underscores

    return sanitized[:250]  # Truncate to 250 to avoid OS limitations (255 characters is a common limit)

def scrape_solicitations(base_url, state, links, save_files=True):
    jsons_list = []
    links_count = len(links)

    for j, link in enumerate(links, start=1):
        sanitized_link = sanitize_filename(link)
        filename = f"{sanitized_link}.json"

        logging.info(f"{j}/{links_count} {sanitized_link}")

        # Create a directory for each link to store the JSON and PDF file
        if not os.path.exists(f'data/{sanitized_link}'):
            os.makedirs(f'data/{sanitized_link}')

        response = requests.get(link)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all 'a' tags with href attributes that end with any of the file extensions
        pattern = re.compile(r'(' + '|'.join(map(re.escape, file_extensions)) + r')$', re.IGNORECASE)
        pdf_links = soup.find_all('a', href=True, title=lambda x: x and pattern.search(x))

        document_links = []

        # Iterate through all found 'a' tags
        for link2 in pdf_links:
            attachment_url = urljoin(base_url, link2['href'])
            attachment_filename = link2.text.strip()  # Using the text of the link as the filename
            posted_date = link2.find_next('td').text.strip()  # Assuming the date is in the next 'td'
            document_links.append((attachment_url, attachment_filename, posted_date))

        document_links = list(set(document_links))  # Remove duplicates if any

        all_texts = []
        logging.info(f'Got {len(document_links)} document link(s)')

        # Loop through the list of downloadable URLs
        for i, (document_url, document_name, document_date) in enumerate(document_links):
            try:
                # Make a HEAD request to get the headers and check the content type
                head_response = requests.head(document_url)
                content_type = head_response.headers.get('Content-Type', '')

                # Normalize the content type
                normalized_content_type = mime_to_extension.get(content_type, 'unknown')

                # Initialize a dictionary to store the document data
                document_data = {
                    'doc_name': document_name,
                    'type': normalized_content_type,
                    'text': None,
                    'document_date': document_date
                }

                # Check the content type and process accordingly
                if normalized_content_type == 'pdf':
                    document_response = requests.get(document_url)
                    pdf = fitz.open(stream=document_response.content, filetype="pdf")
                    text_content = ''
                    for page in pdf:
                        text_content += page.get_text()
                    document_data['text'] = text_content
                    pdf.close()

                elif normalized_content_type == 'xlsx':
                    document_response = requests.get(document_url)
                    xlsx = pd.read_excel(BytesIO(document_response.content))
                    # Convert all the DataFrame to a single string
                    text_content = xlsx.to_csv(header=True, index=False)
                    document_data['text'] = text_content

                elif normalized_content_type == 'docx':
                    document_response = requests.get(document_url)
                    doc = Document(BytesIO(document_response.content))
                    text_content = '\n'.join([paragraph.text for paragraph in doc.paragraphs])
                    document_data['text'] = text_content

                elif normalized_content_type == 'csv':
                    document_response = requests.get(document_url)
                    text_content = document_response.text
                    document_data['text'] = text_content

                all_texts.append(document_data)  # Add the document data to the all_texts list

            except Exception as e:
                print(f"An error occurred while processing {document_url}: {e}")
                continue

        pdf_texts = []

        # Clean up all texts
        for document in all_texts:
            if document['text'] is not None:
                # Replace newlines with spaces
                document['text'] = re.sub('\n+', ' ', document['text'])
                # Remove Unicode sequences
                document['text'] = re.sub('\\\\u+', ' ', document['text'])
                # Remove sequences of underscores
                document['text'] = re.sub('_+', '', document['text'])
                # Remove sequences of commas
                document['text'] = re.sub(',+', '', document['text'])
                # Remove sequences of periods that are longer than 2
                document['text'] = re.sub('\.{2,}', '', document['text'])

                pdf_texts.append(document['text'])

        solicitation_row = soup.find('tr', style=lambda value: value and 'background-color:#E0E0E0' in value)
        td_elements = solicitation_row.find_all('td', style="font-family:'Arial';font-size:8pt")
        solicitation_description = td_elements[1].get_text(strip=True)

        solicitation_number = td_elements[0].get_text(strip=True)
        purchasing_agency = td_elements[2].get_text(strip=True)
        submission_ending_date_time = td_elements[4].get_text(strip=True)
        delivery_point = td_elements[3].get_text(strip=True)
        attachments_table = soup.find(lambda tag: tag.name == "table" and "Attachment Name" in tag.text)
        date_time_posted = attachments_table.find_all('tr')[-1].find_all('td')[-1].get_text(strip=True)

        solicitation_data = {
            "state": "SouthCarolina",
            "main_category": "N/A",
            "solicitation_type": 'Bid',
            "main_title": solicitation_description,
            "solicitation_summary": "N/A",
            "id": solicitation_number,
            "alternate_id": None,
            "status": "Open",
            "due_close_date_est": submission_ending_date_time,
            "due_date_time": submission_ending_date_time,
            "issuing_agency": purchasing_agency,
            "procurement_officer_buyer_name": None,
            "procurement_officer_email": None,
            "additional_instructions": None,
            "issue_date": date_time_posted,
            "pdf_texts": pdf_texts,  # Placeholder for actual PDF content
            "project_cost_class": "N/A",
            "miscellaneous": {"delivery_point": delivery_point},
            "link": link  # Including the link provided
        }

        jsons_list.append(solicitation_data)

        for i, (document_url, document_name, document_date) in enumerate(document_links):
            if save_files:
                document_response = requests.get(document_url)  # Make a request to get the document content
                document_content = document_response.content

                s3_key = f"prod_gold/{state}/{solicitation_number}/documents/"

                # Convert document content to bytes if it's not already in bytes format
                if not isinstance(document_content, bytes):
                    document_content = bytes(document_content, 'utf-8')

                os.makedirs(s3_key, exist_ok=True)
                with open(s3_key + document_name, 'wb') as local_file:
                    local_file.write(document_content)

                # Upload the document to S3
                # s3.put_object(Bucket=bucket_name, Key=s3_key, Body=document_content)
                # logging.info(f"Uploaded {document_name} to S3 at {s3_key}")

        json_s3_key = f"prod_gold/{state}/{solicitation_number}/json/"

        os.makedirs(json_s3_key, exist_ok=True)
        with open(f'{json_s3_key}{solicitation_number}.json', 'w') as json_file:
            json.dump(solicitation_data, json_file, indent=4)

        solicitation_data_bytes = json.dumps(solicitation_data).encode('utf-8')
        # s3.put_object(Bucket=bucket_name, Key=json_s3_key, Body=solicitation_data_bytes)

    return jsons_list

def scrape_links():
    chrome_options = Options()  # Set up the Selenium WebDriver
    chrome_options.add_argument("--headless")  # This argument configures Chrome to run in headless mode.

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    # driver = webdriver.Chrome(options=chrome_options)
    base_url = "https://webprod.cio.sc.gov/SCSolicitationWeb/solicitationSearch.do?d-49653-p=1"

    driver.get(base_url)  # Open the web page

    # Finding and clicking for Open in Solicitation Status
    open_radio_button = driver.find_element(By.XPATH, '//input[@name="searchStatus" and @value="O"]')
    open_radio_button.click()

    # Wait for the "Search" button to be clickable and then click it
    search_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.NAME, "btnSearch"))
    )
    search_button.click()

    page_links = []
    while True:
        # Find all the links in the table with the class 'solicitNumber'
        links = driver.find_elements(By.CSS_SELECTOR, 'td.solicitNumber a')

        # Loop through the links and store the hrefs in the list
        for link in links:
            href = link.get_attribute('href')
            page_links.append(href.lstrip('/'))  # Ensure the href is appended to the base URL correctly

        # Try to find the "Next" link
        try:
            # Find the 'Next' link using the text content of the link
            next_link = driver.find_element(By.XPATH, '//span[@class="pagelinks"]/a[contains(text(), "Next")]')
            next_href = next_link.get_attribute('href')

            # Check if the next_href is a complete URL or a relative path
            if not next_href.startswith('http'):
                next_href = base_url + next_href.lstrip('/')

            # Navigate to the next page
            driver.get(next_href)

            # Wait for the new set of links to be loaded
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'td.solicitNumber a')))

        except NoSuchElementException:
            break  # Break the loop if 'Next' link is not found (i.e., we're on the last page)

        except TimeoutException:
            # If the page takes too long to load, you might want to handle it or break the loop
            logging.error("Page took too long to load or 'Next' link became stale.")
            break

    driver.quit()  # Close the WebDriver after scraping is done

    logging.info(f'Scraped {len(page_links)} links')

    return page_links

def main():
    # Define statical variables
    base_url = 'https://webprod.cio.sc.gov/SCSolicitationWeb/'
    state = 'southcarolina'
    save_files = True

    links = scrape_links()  # Getting all links of solicitations with open status
    links = links[:5]
    solicitations = scrape_solicitations(base_url, state, links, save_files)

    with open(f'prod_gold/solicitations.json', 'w') as json_file:
        json.dump(solicitations, json_file, indent=4)

    logging.info("done :)")

if __name__ == '__main__':
    main()