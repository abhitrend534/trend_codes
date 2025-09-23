import json

with open('brands_list.json', 'r') as file:
    brands_list = json.load(file)

brand_collection_names = {}

for db_type, brands in brands_list.items():
    brand_collection_names[db_type] = {}
    for brand in brands:
        brand_collection_names[db_type][brand] = {
            "product_collection_name": f"v3_products_{brand}",
            "color_collection_name": f"v3_colors_{brand}"
        }
        
# Save to JSON file
file_name = 'brand_collection_names.json'
with open(file_name, 'w') as file:
    json.dump(brand_collection_names, file, indent=4)

print(f'Brands collection names saved to: {file_name}')