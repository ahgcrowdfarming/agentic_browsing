import os
import json
import pandas as pd
from datetime import date
from products import PRODUCTS


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
                            # If the product list is not empty, we process its contents.
                            if data['products']:
                                print(f"  -> Processing {len(data['products'])} product(s) from: {filename}")
                                
                                # Get the product name from the filename (e.g., 'apples' from 'apples.json')
                                product_name_from_file = os.path.splitext(filename)[0]

                                # Loop through each product to apply corrections before adding.
                                for product_dict in data['products']:
                                    # --- FIX 1: Overwrite 'name' with the filename ---
                                    # This ensures the 'name' field is always consistent with the source file.
                                    product_dict['name'] = product_name_from_file

                                    # --- FIX 2: Validate 'subtype' ---
                                    # If 'subtype' is missing, empty, or not a valid option for the product,
                                    # default it to the main product name.
                                    
                                    # Get the list of valid subtypes, defaulting to an empty list if product is not found.
                                    valid_subtypes = PRODUCTS.get(product_name_from_file, [])
                                    
                                    # Create a lowercase version of the list for case-insensitive matching.
                                    valid_subtypes_lower = [subtype.lower() for subtype in valid_subtypes]

                                # Check if subtype exists and is valid.
                                if 'subtype' not in product_dict or not product_dict.get('subtype') or product_dict['subtype'].lower() not in valid_subtypes_lower:
                                    product_dict['subtype'] = product_name_from_file
                                
                                    # --- FIX 3: Add 'scrapped_date' if it's missing ---
                                    # If the date key doesn't exist or is empty, add today's date.
                                    if 'scrapped_date' not in product_dict or not product_dict.get('scrapped_date'):
                                        product_dict['scrapped_date'] = date.today().strftime("%d/%m/%Y")
                                
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
    df = pd.DataFrame(all_products_data)

    # We reorder the columns to make the table more readable.
    # The script will handle it if any of these columns don't exist in the data.
    # 'subtype' was added to this list to ensure it appears in the report.
    desired_columns = [
     'scrapped_date', 'country', 'supermarket_name', 'name', 'subtype', 'website_product_name', 'bio',
     'price_per_kg', 'price_per_unit', 'currency',
     'original_price_info', 'estimation_notes', 'model_used','tokens_used', 'total_cost'
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