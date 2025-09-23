import json
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://tgReader:gfg57656h5Hjhgjlmncg8H7886745-ngRv@3.1.227.250:28018/tg_analytics")

databases = {
    'clothing': 'tg_analytics',
    'footwear': 'footwear_analytics'
}

brands_list = {}

for db_type, db_name in databases.items():
    brands_list[db_type] = {}

    db = client[db_name]
    collection_names = db.list_collection_names()
    brand_collections = [name.replace('crawler_sink_', '')  for name in collection_names if name.startswith('crawler_sink')]
    brand_collections.sort()

    for brand in brand_collections:
        brand_splits = brand.split('_')
        geography = brand_splits[-1]
        brand_name = '_'.join(brand_splits[:-1])
        if brand_name not in brands_list[db_type]:
            brands_list[db_type][brand_name] = [geography]
        else:
            brands_list[db_type][brand_name].append(geography)

# Save to JSON file
file_name = 'brands_list.json'
with open(file_name, 'w') as file:
    json.dump(brands_list, file)

print(f'Brands list saved to: {file_name}')