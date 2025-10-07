import asyncio
import os
import sys
import tempfile
import json
import time
from typing import List, Optional

# --- Setup and Imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm import ChatOpenAI
from browser_use.llm.exceptions import ModelProviderError

load_dotenv()
print("Loaded API key:", os.getenv("OPENAI_API_KEY") is not None)

# --- Pydantic Models (unchanged) ---
class Product(BaseModel):
    name: str
    website_product_name: Optional[str] = None
    price_per_kg: Optional[float] = None
    price_per_unit: Optional[float] = None
    currency: Optional[str] = None
    original_price_info: Optional[str] = None
    estimation_notes: Optional[str] = None
    supermarket_name: str
    country: str
    bio: bool
    packaging_type: Optional[str] = None
    minimum_order_value: Optional[float] = None
    product_origin: Optional[str] = None

class SupermarketOutput(BaseModel):
    products: List[Product]

# --- CORE CONFIGURATION ---
# List of all products to search for.
PRODUCTS = [
    "avocado", "banana", "clementine" ]
    ## "mango", "grape", "pomegranate", "banana", "clementine", "orange", "mandarin", "blood orange", "lemon", "extra virgin olive oil", "kiwi", "persimmon"

# Dictionary of countries and their supermarkets.
COUNTRIES = {
    # "Germany": {
    #     "Rewe": "https://www.google.com/search?q=site:rewe.de+{product}",
    #     # "Lidl": "https://www.lidl.de/c/frische-qualitaet/s10007735",
    #     # "Aldi Nord": "https://www.aldi-nord.de/",
    #     # # "Rewe": "https://www.rewe.de", # Can't get passed the cloudfare verification...
    #     # "Knuspr": "https://www.knuspr.de/",
    #     # "Truebenecker": "https://truebenecker.de/",
    #     # "Jurassic Fruit": "https://www.jurassicfruit.com/en/?country=DE",
    #     # "MyTime.de": "https://www.mytime.de",
    #     # "Supermarkt24h.de": "https://www.supermarkt24h.de/",
    #     # "Stadt Land Frucht": "https://stadt-land-frucht.de/",
    #     # "Tropy.de": "https://tropy.de/"
    #     # Edeka 
    #     # Kaufland
    #     # Amazon Fresh Germany (requires Prime membership)
    # },
    # "France": {
    #     "Carrefour": "https://www.carrefour.fr",
    # #     "Intermarché": "https://www.intermarche.com",
    # #     "LRQDO": "https://laruchequiditoui.fr/fr/assemblies?fullAddress=Paris%2C+France#11/48.8535/2.3484",
    # #     "La Fourche": "https://lafourche.fr/",
    # #     "Le Fourgon" : "https://www.lefourgon.com/",
    # #     "Santafoo": "https://santafoo.fr/",
    # #     #"Auchan": "https://www.auchan.fr",
    # #     "Monoprix": "https://courses.monoprix.fr/",
    # #     "Naturalia": "https://www.naturalia.fr/",
    # #     "Chronodrive": "https://www.chronodrive.com/",
    # #     "Mon Marché": "https://www.monmarche.fr",
    # #     #"Bene Bono": "https://benebono.com/fr",
    # #     "Neary": "https://www.neary.fr",
    # #     "Potager City": "https://www.potagercity.fr"
    # },
    
    # "United Kingdom": {
    #     "Tesco": "https://www.tesco.com/groceries/en-GB/",
    #     "Sainsbury's": "https://www.sainsburys.co.uk",
    #     "Waitrose": "https://www.waitrose.com",
    #     "Co-op": "https://www.coop.co.uk",
    #     "Lidl": "https://www.lidl.co.uk",
    #     "Abel & Cole": "abelandcole.co.uk",
    #     "The Modern Milkman": "https://themodernmilkman.co.uk/",
    #     "Asda": "https://groceries.asda.com",
    #     "Morrisons": "https://groceries.morrisons.com",
    #     "Iceland": "https://www.iceland.co.uk"
    # },
    
    "Spain": {
        "Mercadona": "https://tienda.mercadona.es",
        "Carrefour": "https://www.carrefour.es/supermercado",
        "Dia": "https://www.dia.es",
        "El Corte Inglés": "https://www.elcorteingles.es/supermercado",
        "Lidl": "https://www.lidl.es",
        "Veritas": "https://shop.veritas.es/",
        "Amazon Fresh": "https://www.amazon.es/alm/storefront/fresh?almBrandId=QW1hem9uIEZyZXNo&ref=fs_dsk_sn_logo-5e2a3",
        "Cesta Verde": 	"https://www.cestaverde.com/",
        "Europa Agricult Product": "https://europagricultproduct.com/",
        "Freshish": "https://freshis.com/",
        "Alcampo": "https://compraonline.alcampo.es",
        "Eroski": "https://supermercado.eroski.es",
    },

    # "Italy": {
    #     "Coop": "https://www.coop.it",
    #     "Esselunga": "https://www.esselunga.it",
    #     "Carrefour": "https://www.carrefour.it",
    #     "Conad": "https://www.conad.it",
    #     "Lidl": "https://www.lidl.it",
    #     "Naturasi": "https://www.naturasi.it",
    #     "Cortilia": "https://www.cortilia.it",
    #     "Bennet": "https://www.bennet.com",
    #     "Pam a Casa": "https://pamacasa.pampanorama.it"
    # },

    # "Sweden": {
    #     "ICA": "https://handla.ica.se",
    #     "Mathem": "https://www.mathem.se",
    #     "Grocery Online": "https://groceryonline.se/",
    #     "Coop": "https://www.coop.se/"
    # },
    # "Austria": {
    #     "BILLA": "https://shop.billa.at",
    #     "INTERSPAR": "https://www.interspar.at",
    #     "Spar": "https://www.spar.at",
    #     "MPreis": "https://www.mpreis.at/",
    #     "Gurkel": "https://gurkerl.at/"
    # }


}

# --- PROMPT (unchanged) ---
# Minimum order value en CF. Ej: caja de 2.5 de aguacates.
# Add a variable to see if shipping is included or not. 

PROMPT_TEMPLATE = """
Your task is to extract accurate price information for the product **{product}** from the given **{website_url}**, following the exact sequence and logic below.  
Your ultimate goal is to determine **a comparable price per kilogram (price_per_kg)** for both **bio/organic** and **non-bio/organic** variants (1–2 examples of each).

---

## STEP 1: Access the Website
1. Go directly to: **{website_url}**  
2. If prompted for a postal code, enter a **central postal code for {country}**.  
   - Example: Germany → `10115` (Berlin)
3. 2. If prompted for a postal code, enter a **central postal code for {country}**.  
4. Wait until the product prices are fully visible and loaded.

---

## STEP 2: Search for the Product
- Use the site’s search function to look for **"{product}"**.  
- Identify **1–2 bio/organic** and **1–2 non-bio/organic** items of the product.
- **Only choose the most natural or raw version** of the product (see rules below).

---

## STEP 3: Product Selection Rules (CRITICAL)
- Choose the **pure, unprocessed form** of the product.  
- ❌ DO NOT select processed or derivative versions of the product (e.g., "juice", "smoothie", "dried fruit", "yogurt", "guacamole", "body lotion", "soap", "tea", "soup", etc.) unless explicitly specified in the task (for example, if the product is "apple juice"). Never include these forms on your own unless they are directly requested.
- ✅ You must select the item representing the **base ingredient itself** (e.g., raw mango, raw avocado).

---

## STEP 4: Price Extraction Logic (Follow in Order of Priority)

### **PRIORITY 1 — Direct Price per Kg**
If the price is explicitly shown per kilogram (e.g., `"3,99 € / kg"`):  
- Extract this numeric value directly.  
- Store it in the `"price_per_kg"` field.

---

### **PRIORITY 2 — Calculated Price per Kg (from Weight Info)**
If the product is sold per pack or tray with a stated weight (e.g., `"2,99 € / 750g"`):  
1. Extract total price (e.g., `2.99`) and total weight (e.g., `750g`).  
2. Convert grams to kilograms (e.g., `750g = 0.75kg`).  
3. Calculate `price_per_kg = total_price / weight_in_kg`.  
4. Record the computed value in `"price_per_kg"`.

---

### **PRIORITY 3 — Estimated Price per Kg (from Unit or Pack without Weight Info)**
If the product is sold **per piece or per pack** with **no visible weight** (e.g., `"1 mango 2 €"` or `"3 mangos 5 €"`):  
1. Record the unit price in `"price_per_unit"`.  
2. Use general knowledge to **estimate the average weight** of one unit in kg (e.g., mango ≈ 0.4kg, avocado ≈ 0.17kg).  
3. Calculate `price_per_kg = unit_price / estimated_weight_in_kg`.  
4. Enter this value into `"price_per_kg"`.  
5. Include a short note in `"estimation_notes"` explaining your assumption (e.g., `"Estimated weight for 1 mango is 0.4kg."`).

---

## STEP 5: Data Recording Requirements
For each found product, always record:
- The **name** == {product} 
- The **website product name" as listed on the site.
- The **price per kilogram** (calculated or direct) → store in `"price_per_kg"`.
- The **price per unit** (if applicable) → store in `"price_per_unit"`.
- The **currency** (e.g., EUR, USD) → store in `"currency"`.
- The **original price information** exactly as shown on the site (e.g., `"3,49€ / 6 unidades"` or `"2,99€ / 750g Netz"`) → store in `"original_price_info"`.
- Any **estimation notes** if applicable e.g. 1 mango weighs 0.2kg → store in `"estimation_notes"`.
- The **supermarket name**, **country**, and **bio/organic status** (true/false).
- The **exact URL** of the product page where you found the information (for your own reference, not in the output).
- The supermarket name and country should match the input values.
- Boolean field `"bio"` should be `true` for organic/bio products and `false` for non-organic.
- Packaging type: pick between net, unit, tray, elaborate packaging → store in `"packaging_type"`.
- Minimum order value -> store in `"minimum_order_value"` as INTEGER.
- Product origin (if available) -> store in `"product_origin"`.


---

## STEP 6: Final Output Format
Once all products have been analyzed:

1. **Call the `done` action** only after completing every item.  
2. Provide **ONE single JSON object** as your final output in the `text` argument.  
3. Follow this structure EXACTLY (do not deviate from keys or nesting):

```json
   {{
     "products": [
       {{
            "name": "{product}", 
            "website_product_name": "Mango de Avión",
            "price_per_kg": 5.0,
            "price_per_unit": 2.0,
            "currency": "EUR",
            "original_price_info": "2,00 € / pieza",
            "estimation_notes": "Estimated weight for 1 mango is 0.4kg.",
            "product_url": "https://www.rewe.de/mango124232",
            "supermarket_name": "{supermarket}",
            "country": "{country}",
            "bio": false,
            "packaging_type": "unit",
            "minimum_order_value": 2.0,
            "product_origin": "Spain"
       }},
     ]
   }}
"""

# --- SCRIPT CONTROLS ---
# The number of parallel browser instances to run.
# Start with 2 or 3 to be safe. Increase if your machine and API limits can handle it.
MAX_PARALLEL_BROWSERS = 1

# The number of seconds to wait between starting each new task.
# This is crucial for respecting Tokens-Per-Minute (TPM) limits. 5-10 seconds is a good start.
SECONDS_BETWEEN_TASKS = 4

# The base directory for the final JSON files.
OUTPUT_BASE = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_BASE, exist_ok=True)

# --- Agent Runner (unchanged, it's perfect) ---
async def run_agent_with_retry(agent, output_path, max_retries=3):
    """This function now only contains the retry logic for a single agent run."""
    for attempt in range(max_retries):
        try:
            result = await agent.run()

            if hasattr(result, "structured_output") and result.structured_output:
                print(f"✅ [Agent] Found structured_output from library for {os.path.basename(output_path)}.")
                final_data = result.structured_output.model_dump()
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=2)
                print(f"   -> Saved results to: {output_path}")
                return True

            elif hasattr(result, "final_result") and (final_text := result.final_result()):
                print(f"✅ [Agent] Found final_result text for {os.path.basename(output_path)}. Parsing manually.")
                print(f"DEBUG: Agent final_result text:\n{final_text}")  # <--- Add this line
                try:
                    final_data = json.loads(final_text)
                    SupermarketOutput.model_validate(final_data)
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(final_data, f, ensure_ascii=False, indent=2)
                    print(f"   -> Successfully parsed and saved results to: {output_path}")
                    return True
                except (json.JSONDecodeError, ValidationError) as e:
                    print(f"🚨 [Agent] Final text was not valid JSON for {os.path.basename(output_path)}. Error: {e}")
                    print(f"   -> Final text was: {final_text}")
            else:
                print(f"⚠️ [Agent] No structured output or final text found for {os.path.basename(output_path)} on attempt {attempt + 1}.")
        
        except ModelProviderError as e:
            if "rate_limit_exceeded" in str(e):
                print(f"🚨 [Agent] Rate Limit Error for {os.path.basename(output_path)}. Sleeping for 3 minutes...")
                await asyncio.sleep(180)
            else:
                print(f"🚨 [Agent] OpenAI Error for {os.path.basename(output_path)}: {e}")
                await asyncio.sleep(20) # Shorter sleep for other API errors
        except Exception as e:
            print(f"🚨 [Agent] Unknown critical error during agent run for {os.path.basename(output_path)}: {e}")
            # Break from retries on unknown errors to avoid repeated crashes
            break
            
    # If all retries fail for this agent
    print(f"❌ [Agent] Max retries reached for {os.path.basename(output_path)}. Saving empty results.")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"products": []}, f, ensure_ascii=False, indent=2)
    return False

# --- NEW: Self-Contained Worker ---
async def process_supermarket_task(country, supermarket, product, llm, semaphore):
    """
    This is a self-contained "worker" function. It handles one single task
    (e.g., "avocado at Lidl in Germany") from start to finish, including
    creating and destroying its own browser. This provides maximum isolation and stability.
    """
    async with semaphore:
        task_id = f"{country}_{supermarket}_{product}"
        print(f"▶️  [Worker] Starting task: {task_id}")
        
        # 1. Checkpointing: Check if the task is already done
        output_dir = os.path.join(OUTPUT_BASE, country, supermarket)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{product}.json")

        if os.path.exists(output_path):
            print(f"⏭️  [Worker] Skipping {task_id}, output file already exists.")
            return

        browser_session = None
        try:
            
            browser_session = BrowserSession(
                browser_profile=BrowserProfile(
                    # Headless is better for long, unsupervised runs
                    headless=False,
                    user_data_dir=None,
                    minimum_wait_page_load_time=5,  # Increase wait time (if supported)
                    wait_between_actions=2          # Increase action wait (if supported)
                )
            )
            await browser_session.start()
            print(f"✅ [Worker] Browser started for task: {task_id}")

            # 3. Create and run the agent
            prompt = PROMPT_TEMPLATE.format(
                website_url=COUNTRIES[country][supermarket],
                product=product,
                country=country,
                supermarket=supermarket
            )
            print(f"🧠 [Worker] Created prompt for {task_id}.")
            agent_artifact_dir = os.path.join(tempfile.gettempdir(), "browser_use", task_id)

            agent = Agent(
                task=prompt,
                browser_session=browser_session,
                llm=llm,
                output_model_schema=SupermarketOutput, # This was missing from your Agent() call before
                output_dir=agent_artifact_dir,
                id=task_id
            )
            print(f"🚀 [Worker] created agent for {task_id}.")

            await run_agent_with_retry(agent, output_path)

        except Exception as e:
            print(f"💥 [Worker] A critical unhandled error occurred in task {task_id}: {e}")
            # Ensure an empty file is still created so we don't retry this broken task
            if not os.path.exists(output_path):
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump({"products": []}, f, ensure_ascii=False, indent=2)
        finally:
            # 4. Cleanup: Always ensure the browser instance is killed
            if browser_session:
                await browser_session.kill()
            print(f"⏹️  [Worker] Finished task: {task_id}")

# --- NEW: Task Orchestrator ---
async def main():
    """
    This is the main "Orchestrator". It prepares the list of all jobs,
    and then schedules them to run at a controlled pace.
    """
    llm = ChatOpenAI(model='gpt-4.1-mini', api_key=os.getenv("OPENAI_API_KEY"))
    semaphore = asyncio.Semaphore(MAX_PARALLEL_BROWSERS)

    # Create a list of all jobs to be done
    all_jobs = []
    for country, supermarkets in COUNTRIES.items():
        for supermarket in supermarkets:
            for product in PRODUCTS:
                all_jobs.append((country, supermarket, product))
    
    print(f"Found {len(all_jobs)} total jobs to process.")
    
    tasks = []
    for job in all_jobs:
        country, supermarket, product = job
        
        # Create and schedule the worker task
        task = asyncio.create_task(
            process_supermarket_task(country, supermarket, product, llm, semaphore)
        )
        tasks.append(task)
        
        # Rate Limiting: Wait for a few seconds before scheduling the next one
        await asyncio.sleep(SECONDS_BETWEEN_TASKS)
    
    # Wait for all scheduled tasks to complete
    await asyncio.gather(*tasks)
    print("✅ All tasks have been processed. Script finished.")

if __name__ == "__main__":
    asyncio.run(main())