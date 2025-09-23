import json
from pymongo import MongoClient
 
# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tg_analytics"]

brand = 'nike_acg'

geographies = ['india']
geo_temp = {}

for grography in geographies:
    collection_name = f'crawler_sink_{brand}_{grography.lower()}_footwear'
    print(f'Connecting to {collection_name} now...')
    collection = db[collection_name]

    unique_pids = collection.distinct('product_id')
    print(f'Total {len(unique_pids)} unique pids found.')

    for pid in unique_pids:
        temp = {}
        titles = collection.distinct('title', {'product_id': pid})
        temp['titles'] = titles

        color_ids = collection.distinct('color_id', {'product_id': pid})

        color_temp = {}
        #get color details
        for cid in color_ids:
            color_temp[cid] = {}
            color_names = collection.distinct('color_name', {'color_id': cid})
            color_sizes = collection.distinct('size_name', {'color_id': cid})
            color_urls = collection.distinct('url', {'color_id': cid})
            color_images = collection.distinct('images', {'color_id': cid})
            color_temp[cid]['titles'] = color_names
            color_temp[cid]['sizes'] = color_sizes
            color_temp[cid]['urls'] = color_urls
            color_temp[cid]['images'] = color_images

        temp['colors'] = color_temp
        geo_temp[pid] = temp

    print(geo_temp)

with open(f'{brand}.json', 'w') as file:
    json.dump(geo_temp, file, indent=4)

client.close()