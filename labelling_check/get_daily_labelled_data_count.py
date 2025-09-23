import json
import pandas as pd
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
all_data = []

for db_type, db_name in databases.items():
    db = client[db_name]
    for brand, collections in brand_collection_names[db_type].items():
        brand_name = brand if db_type == 'clothing' else f'{brand}_footwear'
        product_collection_name = collections["product_collection_name"]
        color_collection_name = collections["color_collection_name"]

        print(f"Processing brand: {brand_name}, Product Collection: {product_collection_name}, Color Collection: {color_collection_name}.")

        product_collection = db[product_collection_name]
        color_collection = db[color_collection_name]

        pipeline = [
            {"$match": {"labelling_tool_version": "qwen_openrouter_v.0.1"}},
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$labelled_on"
                        }
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": -1}},
        ]

        product_dates = list(product_collection.aggregate(pipeline))
        color_dates = list(color_collection.aggregate(pipeline))

        for item in product_dates:
            all_data.append({
                "brand": brand_name,
                "type": 'product',
                "labelled_on": item["_id"],
                "document_count": item["count"]
            })

        for item in color_dates:
            all_data.append({
                "brand": brand_name,
                "type": 'color',
                "labelled_on": item["_id"],
                "document_count": item["count"]
            })

# Convert to DataFrame and save
df = pd.DataFrame(all_data)

# Ensure 'labelled_on' is a datetime object
pivot_df = df.pivot_table(
    index='brand',
    columns=['labelled_on', 'type'],
    values='document_count',
    aggfunc='sum',
    fill_value=0
)

# Sort columns by date
pivot_df = pivot_df.sort_index(axis=1, level=0)

# Add total row at the bottom
total_row = pivot_df.sum(numeric_only=True)
total_row.name = 'Total'
pivot_df = pd.concat([pivot_df, total_row.to_frame().T])

# Export to Excel
pivot_df.to_excel('brand_label_summary.xlsx', merge_cells=True)

print("Data processing complete. Summary saved to brand_label_summary.xlsx")