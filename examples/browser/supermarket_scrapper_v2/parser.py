import os
import json
import pandas as pd

# --- CONFIGURATION ---
# The name of the folder containing the results.
INPUT_DIR = 'output'

# The name of the Excel file that will be created.
OUTPUT_FILE = 'supermarket_price_report.xlsx'

def create_excel_report():
    """
    Walks the directory structure, reads all JSON files,
    and compiles the data into a single Excel file.
    """
    # This list will store the data for each product found.
    all_products_data = []
    
    print(f"🔎 Starting scan of directory '{INPUT_DIR}'...")

    # os.walk is perfect for traversing a nested directory structure.
    for root, dirs, files in os.walk(INPUT_DIR):
        for filename in files:
            # We ensure we only process JSON files.
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        # We load the content of the JSON file.
                        data = json.load(f)

                        # The JSON has a 'products' key which contains a list.
                        # We check that it exists and is a list before continuing.
                        if 'products' in data and isinstance(data['products'], list):
                            # If the product list is not empty, we add its contents.
                            if data['products']:
                                print(f"  -> Processing {len(data['products'])} product(s) from: {filename}")
                                # 'extend' adds all items from the list to our main list.
                                all_products_data.extend(data['products'])
                            else:
                                print(f"  -> Empty file (no products): {filename}")
                        else:
                             print(f"  ⚠️  Warning: Invalid format or no 'products' key in {filename}")

                except json.JSONDecodeError:
                    print(f"  🚨 Error: Could not decode JSON in {filename}. The file might be corrupt.")
                except Exception as e:
                    print(f"  🚨 Error: An unexpected error occurred with {filename}: {e}")

    # If we didn't find any data, we exit the script.
    if not all_products_data:
        print("\nNo product data found to process. Exiting.")
        return

    print(f"\n📊 Found a total of {len(all_products_data)} product records.")
    print("Creating data table...")

    # We create a pandas DataFrame from the list of dictionaries.
    # This is the magic of pandas!
    df = pd.DataFrame(all_products_data)

    # We reorder the columns to make the table more readable.
    # The script will handle it if any of these columns don't exist in the data.
    desired_columns = [
        'country', 'supermarket_name', 'name', 'website_product_name', 'bio',
        'price_per_kg', 'estimated_price_per_kg', 'price_per_unit', 'currency',
        'original_price_info', 'estimation_notes'
    ]
    
    existing_columns = [col for col in desired_columns if col in df.columns]
    df = df[existing_columns]

    # We save the DataFrame to an Excel file.
    # index=False prevents pandas from adding an extra column with the row number.
    try:
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"\n✅ Success! The file '{OUTPUT_FILE}' has been created with all the data.")
    except Exception as e:
        print(f"\n🚨 Error while saving the Excel file: {e}")


# --- Script entry point ---
if __name__ == "__main__":
    create_excel_report()