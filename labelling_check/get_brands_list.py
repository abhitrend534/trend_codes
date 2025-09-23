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
    db = client[db_name]

    collection_names = db.list_collection_names()
    brand_collections = ['_'.join(name.split('_')[2:-1])  for name in collection_names if name.startswith('crawler_sink')]
    brand_collections = list(set(brand_collections))
    brand_collections.sort()
    brands_list[db_type] = brand_collections
    print(f'{db_type}: {brand_collections}')

# Save to JSON file
file_name = 'brands_list.json'
with open(file_name, 'w') as file:
    json.dump(brands_list, file, indent=4)

print(f'Brands list saved to: {file_name}')