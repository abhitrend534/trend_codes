import json
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://tgReader:gfg57656h5Hjhgjlmncg8H7886745-ngRv@3.1.227.250:28018/tg_analytics")
db = client["tg_analytics"]

with open('brand_collection_names.json', 'r') as file:
    brand_collection_names = json.load(file)

databases = {
    'clothing': 'tg_analytics',
    'footwear': 'footwear_analytics'
}

# List to hold all records
brand_labelled_details = {}

for db_type, db_name in databases.items():
    db = client[db_name]
    for brand, collections in brand_collection_names[db_type].items():
        brand_name = brand if db_type == 'clothing' else f'{brand}_footwear'
        product_collection_name = collections["product_collection_name"]
        color_collection_name = collections["color_collection_name"]

        print(f"Processing brand: {brand_name}, Product Collection: {product_collection_name}, Color Collection: {color_collection_name}.")

        product_collection = db[product_collection_name]
        color_collection = db[color_collection_name]

        product_total_count = product_collection.count_documents({})
        product_labelled_count = product_collection.count_documents({"is_labelled": True})
        product_unlabelled_count = product_collection.count_documents({"is_labelled": False})
        color_total_count = color_collection.count_documents({})
        color_labelled_count = color_collection.count_documents({"is_labelled": True})
        color_unlabelled_count = color_collection.count_documents({"is_labelled": False})

        brand_labelled_details[brand_name] = {
            "product_total_count": product_total_count,
            "product_labelled_count": product_labelled_count,
            "product_unlabelled_count": product_unlabelled_count,
            "color_total_count": color_total_count,
            "color_labelled_count": color_labelled_count,
            "color_unlabelled_count": color_unlabelled_count
        }

# Save to JSON file
file_name = 'brand_labelled_details.json'
with open(file_name, 'w') as file:
    json.dump(brand_labelled_details, file, indent=4)

print(f'Brands list saved to: {file_name}')