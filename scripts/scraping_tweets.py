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
from PIL import Image, UnidentifiedImageError
from io import BytesIO

load_dotenv()

OUTPUT_DIR = 'images'
MAX_DIMENSION = 720  # px

os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_log(log, print_log=False):
    execute_query("INSERT INTO execution_logs (log) VALUES (%s)", (log,))
    if print_log: print(log)

def download_image(element):
    try:
        img = element.query_selector("xpath=.//img[contains(@class, 'StreamMessageEmbed')]")
        if not img:
            return None
        url = img.get_attribute("src")
        response = requests.get(url)
        response.raise_for_status()
        image_base64 = base64.b64encode(response.content).decode('utf-8')
        return image_base64
    except Exception as e:
        save_log(f"Error on download image: {e}")
        return None
    
def process_and_save(img_data_b64, img_id):
    if not img_data_b64:
        return None
    
    try:
        img_data = base64.b64decode(img_data_b64)
        img = Image.open(BytesIO(img_data))

        w, h = img.size
        max_orig = max(w, h)
        if max_orig > MAX_DIMENSION:
            scale = MAX_DIMENSION / max_orig
            new_size = (int(w * scale), int(h * scale))
            img = img.resize(new_size, Image.LANCZOS)

        fmt = img.format or 'JPEG'
        ext = fmt.lower()

        if img.mode in ("RGBA", "LA") or (fmt.upper() == 'PNG' and 'A' in img.getbands()):
            fmt = 'PNG'
            ext = 'png'
        else:
            fmt = 'JPEG'
            ext = 'jpg'
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')

        filename = f"{img_id}.{ext}"
        filepath = os.path.join(OUTPUT_DIR, filename)

        if fmt == 'JPEG':
            img.save(filepath, fmt, quality=85, optimize=True)
        else:
            img.save(filepath, fmt, optimize=True)

        return filepath

    except (base64.binascii.Error, UnidentifiedImageError) as e:
        save_log(f"[ERROR on process_and_save] id={img_id}: invalid format ({e})")
        return None
    except Exception as e:
        save_log(f"[ERROR on process_and_save] id={img_id}: {e}")
        return None

def scrap_message(page, symbol, total_messages):
    messages = page.query_selector_all("xpath=.//div[contains(@class, 'StreamMessage_container__')]")
    current_count = len(messages)

    for message in messages[total_messages:]:
        try:
            a_element = message.query_selector("xpath=.//a[contains(@href, '/message/')]")
            if not a_element:
                continue
            href_value = a_element.get_attribute("href")
            match = re.search(r"/message/(\d+)", href_value)
            if not match:
                continue
            post_id = match.group(1)

            select_query = f"select id from stocktwits_posts where post_id = {post_id} and symbol = '{symbol}'"
            result = execute_query(select_query)

            time_element = message.query_selector("xpath=.//time")
            date_str = time_element.get_attribute("datetime") if time_element else ""
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ") if date_str else None

            if result is not None and len(result) > 0:
                continue

            author_element = message.query_selector("xpath=.//span[@aria-label='Username']")
            author = author_element.text_content().strip() if author_element else ""

            text_element = message.query_selector("xpath=.//div[starts-with(@class, 'RichTextMessage_body__')]")
            text = text_element.text_content().strip() if text_element else ""

            image_base64 = download_image(message)
            image_path = process_and_save(image_base64, post_id) if image_base64 else None

            counters_span = message.query_selector_all("xpath=.//span[starts-with(@class, 'StreamMessageLabelCount_labelCount')]")

            comments_span = counters_span[0] if counters_span and len(counters_span) > 0 else None
            reshares_span = counters_span[1] if counters_span and len(counters_span) > 1 else None
            likes_span = counters_span[2] if counters_span and len(counters_span) > 2 else None

            comments_count = comments_span.text_content().strip() if comments_span else 0
            reshares_count = reshares_span.text_content().strip() if reshares_span else 0
            likes_count = likes_span.text_content().strip() if likes_span else 0

            comments_count = int(comments_count) if comments_count.isdigit() else 0
            reshares_count = int(reshares_count) if reshares_count and reshares_count.isdigit() else 0
            likes_count = int(likes_count) if likes_count and likes_count.isdigit() else 0

            insert_query = 'insert into stocktwits_posts (symbol, post_id, post_author, post_date, post_text, post_comments, post_reshares, post_likes, post_img_path) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            insert_data = (symbol, post_id, author, date, text, comments_count, reshares_count, likes_count, image_path)

            execute_query(insert_query, insert_data)
        except Exception as e:
            save_log(f"Error on process image: {e}")

    return current_count

def get_symbols():
    select_query = f"SELECT symbol FROM symbols ORDER BY execution_counter, id"
    result = execute_query(select_query)
    return [row['symbol'] for row in result] if result else []

def process_symbol(symbol):
    update_query = f"UPDATE symbols SET execution_counter = execution_counter + 1 WHERE symbol = '{symbol}'"
    execute_query(update_query)

    with sync_playwright() as p:
        SLEEP_TIME = 5
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto('https://stocktwits.com/signin?next=/login')
        time.sleep(SLEEP_TIME)

        page.fill("input[name='login']", os.getenv("STOCKTWITS_USERNAME"))
        page.fill("input[name='password']", os.getenv("STOCKTWITS_PASSWORD"))
        time.sleep(SLEEP_TIME)
        page.press("input[name='password']", "Enter")
        time.sleep(SLEEP_TIME)

        page.goto(f'https://stocktwits.com/symbol/{symbol}')
        time.sleep(SLEEP_TIME)

        last_height = page.evaluate("document.body.scrollHeight")
        total_messages = 0

        while True:
            total_messages = scrap_message(page, symbol, total_messages)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SLEEP_TIME)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                save_log(f"Symbol {symbol}: No more content available to load.", print_log=True)
                break
            last_height = new_height

        browser.close()

def main():
    symbols = get_symbols()
    if not symbols:
        save_log("No symbols found in the database.", print_log=True)
        sys.exit()

    max_workers = int(os.getenv("MAX_WORKERS", 5))
    symbol_iter = iter(symbols)
    future_to_symbol = {}

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit the first 5 tasks
        for _ in range(max_workers):
            try:
                symbol = next(symbol_iter)
                future = executor.submit(process_symbol, symbol)
                future_to_symbol[future] = symbol
                
                save_log(f"Submitting task for symbol: {symbol}", print_log=True)
            except StopIteration:
                break

        # As each task is completed, submit the next one
        while future_to_symbol:
            done, _ = concurrent.futures.wait(
                future_to_symbol, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                symbol_completed = future_to_symbol.pop(future)
                try:
                    future.result()
                    save_log(f"Symbol {symbol_completed} completed successfully.", print_log=True)
                except Exception as exc:
                    save_log(f"Symbol {symbol_completed} raised an exception: {exc}", print_log=True)

                # Submit new task if a symbol is available
                try:
                    next_symbol = next(symbol_iter)
                    new_future = executor.submit(process_symbol, next_symbol)
                    future_to_symbol[new_future] = next_symbol

                    save_log(f"Submitting task for symbol: {symbol}", print_log=True)
                except StopIteration:
                    pass

    save_log(f"Execution finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", print_log=True)


if __name__ == "__main__":
    main()
