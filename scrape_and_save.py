import mysql.connector
from datetime import datetime, timedelta
import json
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Create a global connection object
connection = None


def get_db_connection():
    global connection
    if connection is None or not connection.is_connected():
        connection = mysql.connector.connect(
            host="127.0.0.1", user="root", password="123456", database="test"
        )
        print("NEW CONNECTION ESTABLISHED")
    return connection


def get_items_to_scrape():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT company_id, user_id, email, link, status, last_scrape_date, next_scrape_date, 
           initial_number, current_number, new_items, keywords
    FROM track_company
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
    SET current_number = %s, new_items = %s, next_scrape_date = %s, last_scrape_date = %s
    WHERE company_id = %s;
    """
    cursor.execute(
        query,
        (
            item["current_number"],
            item["new_items"],
            item["next_scrape_date"],
            item["last_scrape_date"],
            item["company_id"],
        ),
    )
    connection.commit()
    cursor.close()


def scrape_website(url, keywords):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)

        # Wait for the page to load and JavaScript to render content
        page.wait_for_load_state("domcontentloaded")

        # Get page text
        text = page.content().lower()
        with open("myfile.txt", "w", encoding="utf-8") as f:
            f.write(text)

        # Count occurrences of keywords
        count = 0
        for keyword in keywords:
            count += text.count(keyword.lower().strip())

        browser.close()

        return count


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


def process_items():
    items = get_items_to_scrape()
    user_items = {}

    for item in items:
        keywords = item["keywords"].split(",")
        current_number = scrape_website(item["link"], keywords)
        if current_number > item["initial_number"]:
            item["new_items"] = current_number - item["initial_number"]
        else:
            item["new_items"] = 0
        item["current_number"] = current_number
        item["last_scrape_date"] = item["next_scrape_date"]
        item["next_scrape_date"] = datetime.now() + timedelta(hours=1)

        update_item(item)

        if item["user_id"] not in user_items:
            user_items[item["user_id"]] = {"email": item["email"], "items": []}
        else:
            user_items[item["user_id"]]["items"].append(
                {"company_name": item["name"], "new_items": item["new_items"]}
            )

        notify = {}

        for x in user_items:
            if len(user_items[x]["items"]) > 0:
                notify[x] = user_items[x]

        store_user_items_for_notification(notify)


if __name__ == "__main__":
    process_items()
