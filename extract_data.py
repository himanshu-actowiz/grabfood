import json
import time
import sys
from utils import read_gzip_files
from db_config import create_table , insert_into_db


base_path = r"C:\Users\hemanshu.marwadi\Desktop\PDP Json's\PDP"
Table = 'grabFood'
BATCH_SIZE = 2000


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
    result['restaurant_time'] = json.dumps(merchant.get('openingHours'))

    restaurant_menu = []

    for categories in merchant.get('menu',{}).get('categories',[]):
        category = {}
        category['categories_name'] = categories.get('name')
        category['categories_id'] = categories.get('ID')
        category['is_available'] = categories.get('available')
        category['items'] = []

        for item in categories.get('items' , []):
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

    result['restaurant_menu'] = json.dumps(restaurant_menu)

    return result


def main(start , end):
    create_table(Table)

    batch = []
    total = 0

    raw = read_gzip_files(base_path,start,end)

    for files in raw:
        result = parsel_data(files)
        if not result:
            continue
        batch.append(result)

        if len(batch) >= BATCH_SIZE:
            insert_into_db(table_name=Table, data=batch)
            total += len(batch)
            batch = []

    if batch:
        insert_into_db(table_name=Table, data=batch)
        total += len(batch)  

    print(f"[{start}-{end}] Total rows inserted: {total}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python main.py <start> <end>")
        sys.exit(1)

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    starting = time.time()
    main(start, end)
    ending = time.time()

    print('Execution Time : ', ending - starting)