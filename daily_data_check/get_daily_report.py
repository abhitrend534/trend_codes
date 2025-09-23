import os
import re
import json
import statistics
from tqdm import tqdm
from fpdf import FPDF
import concurrent.futures
from pymongo import MongoClient
from fpdf.enums import XPos, YPos
from datetime import datetime, timedelta

# MongoDB connection details
connection_string = "mongodb://tgReader:gfg57656h5Hjhgjlmncg8H7886745-ngRv@3.1.227.250:28018/tg_analytics"
# Connect to MongoDB
client = MongoClient(connection_string)

# Database name based on the context
database_names = {
    'clothing': 'tg_analytics',
    'footwear': 'footwear_analytics'
}

# ---------------- PDF Class ----------------
class MyFPDF(FPDF):
    def __init__(self):
        super().__init__()
        font_dir = "dejavu-sans"
        self.add_font("DejaVu", "", os.path.join(font_dir, "DejaVuSans.ttf"))
        self.add_font("DejaVu", "B", os.path.join(font_dir, "DejaVuSans-Bold.ttf"))
        self.set_font("DejaVu", "", 10)

def format_change(text):
    match = re.match(r"([\d.]+)%\s*(increase|decrease)", text.lower())
    if not match:
        return text, None
    percent, direction = match.groups()
    arrow = "▲" if direction == "increase" else "▼"
    color = (0, 150, 0) if direction == "increase" else (200, 0, 0)
    return f"{percent}% {arrow}", color

def is_significant(change):
    match = re.match(r"([\d.]+)", change)
    return match and float(match.group(1)) > 5

def process_collection(collection_name, target_date):
    try:
        collection = db[collection_name]
        splits = collection_name.split('_')
        if len(splits) > 4:
            brand = f'{splits[2]}_{splits[3]}'
        else:
            brand = splits[2]
        geography = splits[-1]

        today_count = collection.count_documents({
            "date_of_scraping": {
                "$gte": target_date,
                "$lt": target_date + timedelta(days=1)
            }
        })

        if today_count == 0:
            return None

        pipeline = [
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_of_scraping"}},
                "count": {"$sum": 1}
            }},
            {"$match": {"_id": {"$lt": target_date.strftime("%Y-%m-%d")}}},
            {"$sort": {"_id": -1}},
            {"$limit": 5}
        ]

        past_data = list(collection.aggregate(pipeline))
        past_counts = {item["_id"]: item["count"] for item in past_data}

        # Mean change with guards for empty/zero baseline
        if past_counts:
            mean_count = statistics.mean(past_counts.values())
            if mean_count == 0:
                mean_change = "No history"
            elif mean_count > today_count:
                decrease_percentage = ((mean_count - today_count) / mean_count) * 100
                mean_change = f"{decrease_percentage:.2f}% decrease"
            elif mean_count < today_count:
                increase_percentage = ((today_count - mean_count) / mean_count) * 100
                mean_change = f"{increase_percentage:.2f}% increase"
            else:
                mean_change = "No change"
        else:
            mean_count = 0
            mean_change = "No history"

        # Last day change with guards
        if past_counts:
            last_date = sorted(past_counts.keys())[-1]
            last_count = past_counts.get(last_date, 0)
            if last_count == 0:
                last_change = "No change" if today_count == 0 else "N/A"
            elif last_count > today_count:
                decrease_percentage = ((last_count - today_count) / last_count) * 100
                last_change = f"{decrease_percentage:.2f}% decrease"
            elif last_count < today_count:
                increase_percentage = ((today_count - last_count) / last_count) * 100
                last_change = f"{increase_percentage:.2f}% increase"
            else:
                last_change = "No change"
        else:
            last_change = "No history"

        return {
            "collection": collection_name,
            "brand": brand,
            "geography": geography,
            "today_count": today_count,
            "past_counts": past_counts,
            "mean_count": mean_count,
            "mean_change": mean_change,
            "last_change": last_change,
        }

    except Exception as e:
        print(f"[ERROR] {collection_name} - {str(e)}")
        return None

def check_collections_parallel(collection_names, target_date, max_workers=10):
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y-%m-%d")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_collection, name, target_date) for name in collection_names]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing collections"):
            result = future.result()
            if result:
                results.append(result)

    return sorted(results, key=lambda x: x.get("collection", 0))

def get_max_font_size(pdf, text, col_width, row_height, max_font=10, min_font=6, font_style=""):
    # Start from the font size based on row height
    font_size = max(min_font, min(max_font, row_height + 1))
    pdf.set_font("DejaVu", font_style, font_size)
    text_width = pdf.get_string_width(str(text))
    # Reduce font size if text is too wide
    while text_width > col_width - 2 and font_size > min_font:
        font_size -= 0.5
        pdf.set_font("DejaVu", font_style, font_size)
        text_width = pdf.get_string_width(str(text))
    return font_size

def create_page_1(today_str, day, pdf, completed, missing, extra):
    # ---------------- Page 1: Summary Table ----------------
    pdf.set_font("DejaVu", "B", 14)
    pdf.cell(0, 10, f"Scraping Report – {today_str} ({day})", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
    pdf.ln(2)

    # Columns and data
    columns = []
    if completed:
        columns.append(("Completed", completed, (198, 239, 206)))
    if missing:
        columns.append(("Missing", missing, (255, 199, 206)))
    if extra:
        columns.append(("Extra", extra, (189, 215, 238)))

    # Handle case with no data
    if not columns:
        pdf.set_font("DejaVu", "", 11)
        pdf.cell(0, 10, "No collections for this day.", new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        return

    max_rows = max(len(col[1]) for col in columns)
    page_height = 270  # available height on A4
    header_height = 16
    row_space = page_height - header_height - 10
    row_height = row_space / max_rows
    row_height = max(4.5, min(8, row_height))
    # font_size = max(6, min(10, row_height + 1))  # replaced by per-cell font size

    # Pad columns
    for _, col_data, _ in columns:
        col_data += [""] * (max_rows - len(col_data))

    # Header
    col_width = 170 // len(columns)
    for name, _, color in columns:
        pdf.set_fill_color(*color)
        header_font_size = get_max_font_size(pdf, name, col_width, row_height, font_style="B")
        pdf.set_font("DejaVu", "B", header_font_size)
        pdf.cell(col_width, row_height, name, border=1, fill=True, align="C")
    pdf.ln()

    # Rows
    for i in range(max_rows):
        for _, col_data, color in columns:
            pdf.set_fill_color(*color)
            cell_text = col_data[i]
            body_font_size = get_max_font_size(pdf, cell_text, col_width, row_height)
            pdf.set_font("DejaVu", "", body_font_size)
            pdf.cell(col_width, row_height, cell_text, border=1, fill=True, align="C")
        pdf.ln()

def create_page_2(pdf, brand_counts):
    # ---------------- Page 2: Significant Changes (>5%) ----------------
    significant = [
        r for r in brand_counts
        if is_significant(r.get("mean_change", "0"))
    ]
    bottom_limit = 277

    if significant:
        pdf.add_page()
        pdf.set_font("DejaVu", "B", 11)
        pdf.cell(0, 10, "Collections with >5% Mean Change",
                new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        pdf.ln(2)

        headers = ["Brand", "Geo", "Today", "Mean", "Last Δ", "Mean Δ", "Past Counts"]
        col_widths = [25, 20, 15, 17, 25, 25, 40]
        pdf.set_font("DejaVu", "B", 9)
        pdf.set_fill_color(210, 230, 255)
        for i, h in enumerate(headers):
            pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        pdf.ln()

        pdf.set_font("DejaVu", "", 8)
        line_height = 4.2

        for row in significant:
            past_lines = [f"{k}: {v}" for k, v in row["past_counts"].items()]
            past_str = "\n".join(past_lines)
            num_lines = len(past_lines)
            line_height = 4.2
            row_height = num_lines * line_height

            # Prepare content (truncate if too long)
            def clip(text, max_len):
                return text[:max_len - 1] + "…" if len(text) > max_len else text

            # Build brand: keep marker on a new line without clipping it away
            base_brand = row.get("brand", "")
            if row.get("database") == "footwear":
                brand = clip(base_brand, 18) + "\n(footwear ★)"
            else:
                brand = clip(base_brand, 18)
            geo = clip(row.get("geography", ""), 12)
            today = str(row.get("today_count", ""))
            mean = f"{row.get('mean_count', 0):.1f}"
            last_text, last_color = format_change(row.get("last_change", ""))
            mean_text, mean_color = format_change(row.get("mean_change", ""))

            # Ensure the row is tall enough to fit brand's lines
            brand_lines = max(1, brand.count("\n") + 1)
            row_height = max(row_height, brand_lines * line_height)

            # Check for page break
            if pdf.get_y() + row_height > bottom_limit:
                pdf.add_page()
                # redraw header
                pdf.set_font("DejaVu", "B", 9)
                pdf.set_fill_color(210, 230, 255)
                for i, h in enumerate(headers):
                    pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
                pdf.ln()
                pdf.set_font("DejaVu", "", 8)

            # Save start position
            x = pdf.get_x()
            y = pdf.get_y()

            # Column 1: Brand (allow multi-line so footwear marker can be on next line)
            # Vertically center the brand within the row box
            brand_text_height = brand_lines * line_height
            brand_y = y + (row_height - brand_text_height) / 2
            pdf.set_xy(x, brand_y)
            pdf.multi_cell(col_widths[0], line_height, brand, border=0, align="C")
            # Draw a bounding box to match the full row height
            pdf.rect(x, y, col_widths[0], row_height)
            x += col_widths[0]
            # Reset cursor to top of the row for the next column
            pdf.set_xy(x, y)

            # Column 2: Geography
            pdf.set_xy(x, y)
            pdf.cell(col_widths[1], row_height, geo, border=1, align="C")
            x += col_widths[1]

            # Column 3: Today
            pdf.set_xy(x, y)
            pdf.cell(col_widths[2], row_height, today, border=1, align="C")
            x += col_widths[2]

            # Column 4: Mean
            pdf.set_xy(x, y)
            pdf.cell(col_widths[3], row_height, mean, border=1, align="C")
            x += col_widths[3]

            # Column 5: Last Change (arrow)
            pdf.set_xy(x, y)
            if last_color:
                pdf.set_text_color(*last_color)
            pdf.cell(col_widths[4], row_height, last_text, border=1, align="C")
            pdf.set_text_color(0, 0, 0)
            x += col_widths[4]

            # Column 6: Mean Change (arrow)
            pdf.set_xy(x, y)
            if mean_color:
                pdf.set_text_color(*mean_color)
            pdf.cell(col_widths[5], row_height, mean_text, border=1, align="C")
            pdf.set_text_color(0, 0, 0)
            x += col_widths[5]

            # Column 7: Past Counts (multi-line, aligned to center for readability)
            pdf.set_xy(x, y)
            pdf.multi_cell(col_widths[6], line_height, past_str, border=1, align="C")

            # Move to next row
            pdf.set_y(y + row_height)

def generate_report(today_str, day, brand_counts, database_names):
    # Build target display names per DB side using JSON structure
    clothing_targets = set()
    footwear_targets = set()

    if isinstance(database_names, dict):
        clothing_map = database_names.get("clothing", {})
        footwear_map = database_names.get("footwear", {})

        if day in clothing_map:
            for _, collections in clothing_map[day].items():
                clothing_targets.update(collections)
        if day in footwear_map:
            for _, collections in footwear_map[day].items():
                footwear_targets.update(collections)

    # Targets to display: footwear suffixed so we distinguish DB side
    target_display = set(clothing_targets)
    target_display.update({name + " (footwear ★)" for name in footwear_targets})

    # Actual display names from results (already suffixed for footwear)
    actual_display = set(
        [entry.get("display_collection", entry.get("collection")) for entry in brand_counts]
    )

    missing_display = sorted(list(target_display - actual_display))
    extra_display = sorted(list(actual_display - target_display))
    completed_display = sorted(list(target_display & actual_display))

    # ---------------- Start PDF ----------------
    pdf = MyFPDF()
    pdf.add_page()

    create_page_1(today_str, day, pdf, completed_display, missing_display, extra_display)
    create_page_2(pdf, brand_counts)

    return pdf

if __name__ == "__main__":
    # Load the day-specific collection mapping
    with open('database_names.json', 'r') as f:
        database_names_json = json.load(f)

    today_str = datetime.now().strftime("%Y-%m-%d")
    day = datetime.strptime(today_str, "%Y-%m-%d").strftime('%A')
    print(f"Starting to check collections for {today_str} data...")

    # Prepare to collect all collection names and results from both DBs
    all_collection_names = []
    all_brand_counts = []

    for database_type, database_name in database_names.items():
        print(f'Processing {database_type} database: {database_name}')
        db = client[database_name]
        collections = db.list_collection_names()
        collection_names = [name for name in collections if name.startswith('crawler_sink_')]
        collection_names.sort()
        all_collection_names.extend(collection_names)
        # Ensure global db is set for processing
        globals()['db'] = db
        # Process normally for both DBs; don't mutate collection names for DB access
        brand_counts = check_collections_parallel(collection_names, today_str)
        # Tag results with database type and compute display name
        for entry in brand_counts:
            entry['database'] = database_type
            entry['display_collection'] = (
                entry['collection'] + " (footwear ★)"
                if database_type == 'footwear'
                else entry['collection']
            )
        all_brand_counts.extend(brand_counts)

    pdf = generate_report(today_str, day, all_brand_counts, database_names_json)

    # make sure the reports directory exists
    os.makedirs("daily_reports", exist_ok=True)
    pdf_path = f"daily_reports/report_{today_str}.pdf"
    pdf.output(pdf_path)
    print(f"✅ PDF generated: {pdf_path}")

    # Close the MongoDB connection
    client.close()