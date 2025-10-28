import os
import re
import sys
sys.path.insert(0, '../')
import time
import base64
import requests
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from config.database import execute_query
from playwright.sync_api import sync_playwright
import numpy as np

load_dotenv()

def get_posts():
    query = "SELECT * FROM stocktwits_posts WHERE post_likes IS NULL ORDER BY id"
    result = execute_query(query)
    return result if result else []

def update_post_metrics(id, comments, reshares, likes):
    update_query = """
        UPDATE stocktwits_posts 
        SET post_comments = %s, post_reshares = %s, post_likes = %s 
        WHERE id = %s
    """
    execute_query(update_query, (comments, reshares, likes, id))

def partition_ranges(db_size, parts=10):
    ranges = []
    base_size = db_size // parts
    remainder = db_size % parts
    start = 1
    for i in range(parts):
        extra = 1 if i < remainder else 0
        end = start + base_size + extra - 1
        ranges.append((start, end))
        start = end + 1
    return ranges

def process_post_metrics(posts):
    SLEEP_TIME = 4
    N_RESTART = 100  # restart browser after 100 posts
    count = 0

    def start_browser():
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        # Login
        page.goto('https://stocktwits.com/signin?next=/login')
        time.sleep(SLEEP_TIME)
        page.fill("input[name='login']", os.getenv("STOCKTWITS_USERNAME"))
        page.fill("input[name='password']", os.getenv("STOCKTWITS_PASSWORD"))
        time.sleep(SLEEP_TIME)
        page.press("input[name='password']", "Enter")
        time.sleep(SLEEP_TIME)
        return playwright, browser, context, page

    playwright, browser, context, page = start_browser()

    if not posts:
        print("No more posts to process.")
        return

    for post in posts:
        try:
            post_id = post['post_id']
            author = post['post_author']
            print(f"Processing post ID: {post_id} by {author}")

            page.goto(f'https://stocktwits.com/{author}/message/{post_id}')
            time.sleep(3)

            message_element = page.query_selector(f"xpath=.//div[@data-testid='message-{post_id}']")
            if not message_element:
                update_post_metrics(post['id'], 0, 0, 0)
                continue

            counters_span = message_element.query_selector_all("xpath=.//span[starts-with(@class, 'StreamMessageLabelCount_labelCount')]")

            def parse_count(txt):
                if not txt:
                    return 0
                txt = txt.lower().replace(',', '')
                if 'k' in txt:
                    return int(float(txt.replace('k','')) * 1000)
                if txt.isdigit():
                    return int(txt)
                return 0

            if counters_span:
                comments_span = counters_span[0]
                reshares_span = counters_span[1] if len(counters_span) > 1 else None
                likes_span = counters_span[2] if len(counters_span) > 2 else None

                comments_count = parse_count(comments_span.text_content().strip()) if comments_span else 0
                reshares_count = parse_count(reshares_span.text_content().strip()) if reshares_span else 0
                likes_count = parse_count(likes_span.text_content().strip()) if likes_span else 0

                update_post_metrics(
                    post['id'],
                    comments_count,
                    reshares_count,
                    likes_count
                )
            else:
                update_post_metrics(post['id'], 0, 0, 0)
        except Exception as _:
            update_post_metrics(post['id'], 0, 0, 0)
        count += 1

        if count % N_RESTART == 0:
            page.close()
            context.close()
            browser.close()
            playwright.stop()
            time.sleep(3)
            playwright, browser, context, page = start_browser()

    page.close()
    context.close()
    browser.close()
    playwright.stop()


def get_db_size():
    db_size_query = "SELECT COUNT(*) as count FROM stocktwits_posts"
    db_size_result = execute_query(db_size_query)
    return db_size_result[0]['count'] if db_size_result else 0

def main():
    MAX_WORKERS = 10
    posts = get_posts()
    posts_set = [arr.tolist() for arr in np.array_split(posts, MAX_WORKERS)]

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_range = {executor.submit(process_post_metrics, p): p for p in posts_set}
        while future_to_range:
            done, _ = concurrent.futures.wait(future_to_range, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                r = future_to_range.pop(future)
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing post metrics for range {r}: {e}")
                    # Resubmit the failed range
                    # new_future = executor.submit(process_post_metrics, r)
                    # future_to_range[new_future] = r
    

if __name__ == "__main__":
    main()