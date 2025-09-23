import json
import csv

with open('sorted_footwear_db_dates_india copy.json', 'r') as file:
    data = json.load(file)

with open('india-footwear_dates.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['brand', 'date'])
    for brand, date in data.items():
        writer.writerow([brand.replace('_footwear', ''), date])



