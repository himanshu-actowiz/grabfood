import json
import logging
from threading import Thread
from utils import read_gzip_files
from db_config import create_table , insert_into_db


logging.basicConfig(
    level=logging.INFO,
    filename='file_info.log',
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

#DIR PATH
base_path = r"C:\Users\hemanshu.marwadi\Desktop\PDP Json's\PDP"
Table = 'grabFood'

#Every single Query run for 2k batch
BATCH_SIZE = 2000


#Data Parse
def parsel_data(json_data):

    merchant = json_data.get('merchant')

    if not merchant:
        return None

    result = {}

    result['restaurant_name'] = merchant.get('name')
    result['restaurant_id'] = merchant.get('ID')
    result['restaurant_cuisine'] = merchant.get('cuisine')
    result['restaurant_IMG'] = merchant.get('photoHref')
    result['restaurant_timeZone'] = merchant.get('timeZone')

    #unicode Fix
    result['restaurant_time'] = json.dumps(
        merchant.get('openingHours'),
        ensure_ascii=False
    )

    restaurant_menu = []

    for categories in merchant.get('menu', {}).get('categories', []):

        category = {}
        category['categories_name'] = categories.get('name')
        category['categories_id'] = categories.get('ID')
        category['is_available'] = categories.get('available')
        category['items'] = []

        for item in categories.get('items', []):

            item_data = {}
            item_data['item_id'] = item.get('ID')
            item_data['item_name'] = item.get('name')
            item_data['is_available'] = item.get('available')
            item_data['item_description'] = item.get('description')
            item_data['item_IMG'] = item.get('imgHref')
            item_data['item_price'] = item.get('takeawayPriceInMin')
            item_data['item_discounted_price'] = item.get('discountedTakeawayPriceInMin')

            category['items'].append(item_data)

        restaurant_menu.append(category)

    #unicode fix
    result['restaurant_menu'] = json.dumps(
        restaurant_menu,
        ensure_ascii=False
    )

    return result


#main fun execute operation
def main(start , end):

    batch = []
    total = 0

    raw = read_gzip_files(base_path,start,end)

    for files in raw:

        result = parsel_data(files)

        if not result:
            continue

        batch.append(result)

        if len(batch) >= BATCH_SIZE:

            logging.info(f"Insert Query | Table: {Table} | Rows: {len(batch)}")

            insert_into_db(
                table_name=Table,
                data=batch
            )

            total += len(batch)
            batch = []

    if batch:

        insert_into_db(
            table_name=Table,
            data=batch
        )

        total += len(batch)

    logging.info(f"[{start}-{end}] Total rows inserted: {total}")


if __name__ == '__main__':

    logging.info("Script started")

    create_table(Table)

    #use Thread's
    threads = []

    step = 10000
    total_files = 60000

    for start in range(0, total_files, step):

        end = start + step

        logging.info(f"Thread started for range {start}-{end}")

        target_obj = Thread(
            target=main,
            args=(start, end)
        )

        threads.append(target_obj)

        target_obj.start()

    for t in threads:
        t.join()