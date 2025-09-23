import json
from datetime import datetime
from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
# Connect to MongoDB
client = MongoClient(connection_string)

# Database name based on the context
database_names = {
    'clothing': 'tg_analytics',
    'footwear': 'footwear_analytics',
}

# Initialize containers
target_prefix = 'crawler_sink_'
all_brand_dates = {}  # Store results from all databases

for database_type, database_name in database_names.items():
    print(f'Processing {database_type} database: {database_name}')
    db = client[database_name]

    collection_names = db.list_collection_names()
    # Filter and sort collection names
    collection_names = [name for name in collection_names if name.startswith(target_prefix)]
    collection_names.sort()

    # Step 1: Fetch dates per collection and extract brands
    date_dict = {}
    brands_set = set()
    
    for collection_name in collection_names:
        print(f'Fetching dates from {collection_name}...')
        collection = db[collection_name]

        unique_dates = collection.distinct('date_of_scraping')
        formatted_dates = [
            d.strftime('%Y-%m-%d') for d in unique_dates if isinstance(d, datetime)
        ]
        formatted_dates.sort()

        date_dict[collection_name] = formatted_dates
        print(f'{collection_name} : {formatted_dates}')

        # Extract brand from collection name
        parts = collection_name.split('_')
        if len(parts) > 2:
            if len(parts) == 5:
                brands_set.add(f"{parts[2]}_{parts[3]}")
            else:
                brands_set.add(parts[2])

    # Step 2: Filter by brand name and consolidate dates
    brand_dates = {}
    for collection_name, dates in date_dict.items():
        for brand in brands_set:
            if f'{target_prefix}{brand}' in collection_name:
                if database_type == 'clothing':
                    brand_dates.setdefault(brand, []).extend(dates)
                elif database_type == 'footwear':
                    brand_dates.setdefault(f'{brand}_footwear', []).extend(dates)

    # Step 3: Deduplicate and sort per brand
    for brand in brand_dates:
        brand_dates[brand] = sorted(set(brand_dates[brand]))
        print(brand, brand_dates[brand])
    
    # Step 4: Accumulate results from this database
    for brand, dates in brand_dates.items():
        if brand in all_brand_dates:
            all_brand_dates[brand].extend(dates)
        else:
            all_brand_dates[brand] = dates

# Step 5: Final deduplication and sorting across all databases
for brand in all_brand_dates:
    all_brand_dates[brand] = sorted(set(all_brand_dates[brand]))
    print(f"Final {brand}: {all_brand_dates[brand]}")

# Save sorted DB dates
sorted_brand_dates = dict(sorted(all_brand_dates.items()))
with open('sorted_db_dates.json', 'w') as f:
    json.dump(sorted_brand_dates, f, indent=4)

# Close MongoDB connection
client.close()