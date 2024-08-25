import mysql.connector
from datetime import datetime, timedelta
import json
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncio
import re

# Create a global connection object
connection = None
counter = 0
no_access_items = ['cloudflare', 'wordpress']

def get_db_connection():
    global connection
    try:
        if connection is None or not connection.is_connected():
            connection = mysql.connector.connect(
                host="10.0.0.74", user="root1", password="123456", database="test"
            )
            print("NEW CONNECTION ESTABLISHED")
        
        return connection

    except Exception as e:
        print(f"Couldn't connect to the database: {e}")
        


def get_items_to_scrape():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT company_id, user_id, email, link, status, last_scrape_date, next_scrape_date, 
           initial_number, current_number, new_items, keywords, status
    FROM track_company WHERE status = 1
    LIMIT 100;
    """
    cursor.execute(query)
    items = cursor.fetchall()
    cursor.close()
    return items


def update_item(item):
    connection = get_db_connection()
    cursor = connection.cursor()
    query = """
    UPDATE track_company
    SET current_number = %s, new_items = %s, next_scrape_date = %s, last_scrape_date = %s, status = %s, company_name = %s
    WHERE company_id = %s;
    """
    cursor.execute(
        query,
        (
            item["current_number"],
            item["new_items"],
            item["next_scrape_date"],
            item["last_scrape_date"],
            item["status"],
            item["company_name"],
            item["company_id"]
        ),
    )
    connection.commit()
    cursor.close()


def scrape_website_soap(url, keywords):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    text = soup.get_text().lower()
    company_name = url.split('.')[1].replace('/', '-')

    with open( company_name + '.txt', "w", encoding="utf-8") as f:
        f.write(text)

    count = 0
    for keyword in keywords:
        count += text.count(keyword.lower().strip())
        global no_access_items

        status = 1
        for d_item in no_access_items:
            if text.count(d_item.lower().strip()):
                status = 0;
    
    return [count, company_name, status]

def store_user_items_for_notification(user_items):
    connection = get_db_connection()
    cursor = connection.cursor()
    for user_id, data in user_items.items():
        cursor.execute(
            """
            INSERT INTO user_notification (user_id, email, items, processed)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE items = VALUES(items), processed = VALUES(processed)
        """,
            (user_id, data["email"], json.dumps(data["items"]), False),
        )
    connection.commit()
    cursor.close()



async def scrape_and_find_keywords(page, url, keywords):
    matching_keywords = []
    try:
        # Navigate to the URL
        await page.goto(url)

        # Wait for network idle state to ensure page has loaded completely
        await page.wait_for_load_state('networkidle')

        # Get the full page text content
        text_content = await page.content()

        # Optional: Save the content to a file for debugging purposes
        safe_filename = url.split("//")[-1].split('/')[0].replace('.', '_') + '.txt'
        with open(safe_filename, "w", encoding="utf-8") as f:
            f.write(text_content)

        
        for na_item in no_access_items:
            if na_item in text_content:
                return [na_item]

        # Loop through the keywords and find matches
        for keyword in keywords:
            # Escape special characters in keyword for regex
            escaped_keyword = re.escape(keyword.strip())
            # Use regex to find all matches of the keyword in the text content
            matches = re.findall(escaped_keyword, text_content)
            if matches:
                # Append the keyword to the matching_keywords list for each match found
                matching_keywords.extend([keyword] * len(matches))


    except Exception as e:
        print(f"Error scraping {url}: {e}")

    return matching_keywords


async def scrape_website_playwright(items):
    new_items = []
    try:
        async with async_playwright() as p:
            # Launch a single browser instance
            browser = await p.chromium.launch(headless=True)
            tasks = []

            # Create a page per URL and scrape concurrently
            for item in items:
                try:
                    page = await browser.new_page()
                    task = scrape_and_find_keywords(page, item["link"], item["keywords"].split(","))
                    tasks.append(task)
                except Exception as e:
                    print(f"Error creating page for {item['link']}: {e}")

            # Gather results from all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process the results and update the item data
            for item, result in zip(items, results):
                if isinstance(result, Exception):
                    print(f"Error processing {item['link']}: {result}")
                    item["current_number"] = 0
                    item["matched_items"] = []
                else:
                    item["current_number"] = len(result)
                    item["matched_items"] = result
                    item["company_name"] = re.sub(r'[^a-zA-Z]', '', item["link"].split('.')[1])
                    new_items.append(item)
                    for na_item in no_access_items:
                        if na_item in result:
                            item["status"] = 3


            # for item, result in zip(items, results):
            #     if isinstance(result, Exception):
            #         print(f"Error processing {item['link']}: {result}")
            #         item["current_number"] = 0
            #         item["matched_items"] = []
            #     else:
            #         item["current_number"] = len(result)
            #         item["matched_items"] = result
            #         item["company_name"] = re.sub(r'[^a-zA-Z]', '', item["link"].split('.')[1])

            # Close the browser instance
            await browser.close()

    except Exception as e:
        print(f"General scraping error: {e}")

    return new_items


async def process_items():
    items = get_items_to_scrape()  # Ensure this function works properly
    if not items:
        print("No items to scrape.")
        return

    print(f"Scraping {len(items)} items...")
    scrape_results = await scrape_website_playwright(items)

    for item in scrape_results:
        item["new_items"] = 0
        item["last_scrape_date"] = item["next_scrape_date"]
        item["next_scrape_date"] = datetime.now() + timedelta(hours=1)
        if item["current_number"] > item["initial_number"]:
            item["new_items"] = item["current_number"] - item["initial_number"]

        # Log item data for debugging
        for key, value in item.items():
            print(f"{key}: {value}")

        # Update item in the database
        update_item(item)

    # Uncomment this section if user email notifications are needed
    # user_items = {}
    # for item in scrape_results:
    #     if item["user_id"] not in user_items:
    #         user_items[item["user_id"]] = {"email": item["email"], "items": []}
    #     user_items[item["user_id"]]["items"].append(
    #         {"company_name": item["current_company"], "new_items": item["new_items"]}
    #     )

    # notify = {user_id: user_items[user_id] for user_id in user_items if len(user_items[user_id]["items"]) > 0}
    # store_user_items_for_notification(notify)


# To run the async function in a synchronous context
if __name__ == "__main__":
    asyncio.run(process_items())
