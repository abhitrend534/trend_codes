import json
from datetime import datetime

with open('pipeline_dates.json', 'r') as file:
    pipeline_data = json.load(file)

with open('sorted_db_dates.json', 'r') as file:
    sorted_brand_dates = json.load(file)

pipeline_comparison = {}

end_date = datetime.strptime("2025-09-20", "%Y-%m-%d")
for brand, pipeline_date_str in pipeline_data.items():
    if brand in sorted_brand_dates:
        try:
            pipeline_date = datetime.strptime(pipeline_date_str, "%Y-%m-%d")
            for date_str in sorted_brand_dates[brand]:
                db_date = datetime.strptime(date_str, "%Y-%m-%d")
                if db_date > pipeline_date and db_date < end_date:
                # if db_date > pipeline_date:
                    pipeline_comparison.setdefault(brand, []).append(date_str)
        except ValueError:
            print(f"Invalid date format for brand '{brand}': {pipeline_date_str}")

# Save the comparison results
filename = 'pipeline_to_run_dates.json'
with open(filename, 'w') as f:
    json.dump(pipeline_comparison, f, indent=4)

print(f'Pipeline date file created and saved to: {filename}')