import asyncio
import os
import sys
import tempfile
import json
import time
from typing import List, Optional
import datetime
from datetime import date

# --- Setup and Imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm import ChatOpenAI
from browser_use.llm.exceptions import ModelProviderError
from openai import OpenAI



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
    # minimum_order_value: Optional[float] = None
    product_origin: Optional[str] = None
    # product_url: Optional[str] = None
    is_discounted: Optional[bool] = None
    discount_details: Optional[str] = None
    # data_scraped_at: Optional[str] = None

class SupermarketOutput(BaseModel):
    products: List[Product]

# --- CORE CONFIGURATION ---
# List of all products to search for.
PRODUCTS = [
    "avocado", "banana", "clementine" ]
    ## "mango", "grape", "pomegranate", "banana", "clementine", "orange", "mandarin", "blood orange", "lemon", "extra virgin olive oil", "kiwi", "persimmon"

# Dictionary of countries and their supermarkets.
COUNTRIES = {
    "Germany": {
    #     "Rewe": "https://www.google.com/search?q=site:rewe.de+{product}",
    #     # "Lidl": "https://www.lidl.de/c/frische-qualitaet/s10007735",
    #     # "Aldi Nord": "https://www.aldi-nord.de/",
    #     # # "Rewe": "https://www.rewe.de", # Can't get passed the cloudfare verification...
    #     # "Knuspr": "https://www.knuspr.de/",
        "Truebenecker": "https://truebenecker.de/",
    #     # "Jurassic Fruit": "https://www.jurassicfruit.com/en/?country=DE",
    #     # "MyTime.de": "https://www.mytime.de",
    #     # "Supermarkt24h.de": "https://www.supermarkt24h.de/",
    #     # "Stadt Land Frucht": "https://stadt-land-frucht.de/",
    #     # "Tropy.de": "https://tropy.de/"
    #     # Edeka 
    #     # Kaufland
    #     # Amazon Fresh Germany (requires Prime membership)
    },
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
    
    # "Spain": {
    #     # "Mercadona": "https://tienda.mercadona.es",
    #     "Carrefour": "https://www.carrefour.es/supermercado",
    #     "Dia": "https://www.dia.es",
    #     # "El Corte Inglés": "https://www.elcorteingles.es/supermercado",
    #     # "Lidl": "https://www.lidl.es",
    #     # "Veritas": "https://shop.veritas.es/",
    #     # "Amazon Fresh": "https://www.amazon.es/alm/storefront/fresh?almBrandId=QW1hem9uIEZyZXNo&ref=fs_dsk_sn_logo-5e2a3",
    #     # "Cesta Verde": 	"https://www.cestaverde.com/",
    #     # "Europa Agricult Product": "https://europagricultproduct.com/",
    #     # "Freshish": "https://freshis.com/",
    #     # "Alcampo": "https://compraonline.alcampo.es",
    #     # "Eroski": "https://supermercado.eroski.es",
    # },

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

PROMPT_TEMPLATE = """
Your task: extract the **price per kilogram (price_per_kg)** of **{product}'s** from **{website_url}**, using the exact logic below. 

---

## STEP 1: Access Website
1. Open **{website_url}**.  
2. If asked for postal code, use a **central {country} code** (e.g., Germany → `10115`).
3. If asked to pick a store within that postocode, choose the **first option**.  
4. Wait until prices fully load.

---

## STEP 2: Find Product
- Search for **"{product}"**.  
- Choose the **pure base ingredient** (e.g., raw avocado, banana. and NOT processed forms e.g. banana smoothie, unless explicitly specified in this prompt.).  
- Try to find at least 2 **bio/organic** and 2 **non-bio** results. If none found, proceed with what is available.
---

## STEP 3: Price Extraction (Priority Order)

**1️⃣ Direct per-kg price:**  
If shown (e.g., “3,99 €/kg”), record value in `"price_per_kg"`.

**2️⃣ Derived per-kg price (from weight):**  
If shown as “2,99 €/750g”:  
- Extract total price and weight.  
- Convert to kg → compute `price_per_kg = total_price / weight_kg`.  

**3️⃣ Estimated per-kg price (no weight info):**  
If sold per piece or pack (e.g., “1 mango 2€”):  
- Record unit price.  
- Estimate weight from general knowledge (e.g., mango ≈ 0.4 kg).  
- Compute `price_per_kg = price_per_unit / estimated_weight`.  
- Add `"estimation_notes"` (e.g., “1 mango ≈ 0.4 kg”).

---

## STEP 5: Record Data Fields
For each product, include:

- `"name"`: {product}  
- `"website_product_name"`: as listed  
- `"price_per_kg"`: (direct or calculated)  
- `"price_per_unit"`: (if any)  
- `"currency"`: EUR, GBP, etc.
- `"original_price_info"`: exact site text  
- `"estimation_notes"`: (if used)  
- `"supermarket_name"` and `"country"`  
- `"bio"`: true/false  
- `"packaging_type"`: one of  
  ["net","loose/unit","bag","punnet","tray","loose with film wrap","pre-cut portions","jar / bottle","others"]  
- `"product_origin"` (if visible)  
- `"is_discounted"`: true/false  
- `"discount_details"`: description if discounted

---

## STEP 6: Output Format
After completing all items:

- Call `done` once.  
- Output **ONE JSON object** only, no file edits.  
- Use exactly this format:


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
            "supermarket_name": "{supermarket}",
            "country": "{country}",
            "bio": false,
            "packaging_type": "loose",
            "product_origin": "Spain",
            "is_discounted": true,
            "discount_details": "10% off for members",
       }},
     ]
   }}
"""
# Once you have provided the final JSON output in the required format, **call the `done` action and STOP. Do NOT repeat or re-evaluate the page. Do NOT call `done` more than once. The task is finished after the first successful `done` call.**

# --- SCRIPT CONTROLS ---
# The number of parallel browser instances to run.
# Start with 2 or 3 to be safe. Increase if your machine and API limits can handle it.
MAX_PARALLEL_BROWSERS = 1

# The number of seconds to wait between starting each new task.
# This is crucial for respecting Tokens-Per-Minute (TPM) limits. 5-10 seconds is a good start.
SECONDS_BETWEEN_TASKS = 1

# The base directory for the final JSON files.
OUTPUT_BASE = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_BASE, exist_ok=True)

# --- Agent Runner (unchanged, it's perfect) ---
async def run_agent_with_retry(agent, output_path, max_retries=1):
    """This function now only contains the retry logic for a single agent run."""
    for attempt in range(max_retries):
        print(f"🔄 [Agent] Attempt {attempt + 1} for {os.path.basename(output_path)}.")
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
                    print(f"🧩 [Agent Debug] Output model validated successfully.")
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
                    # minimum_wait_page_load_time=2,  # Increase wait time (if supported)
                    # wait_between_actions=0.5          # Increase action wait (if supported)
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
                id=task_id,
                max_steps = 40
            )
            print(f"🚀 [Worker] created agent for {task_id}.")

            await run_agent_with_retry(agent, output_path)
            print(f"✅ [Worker] Agent finished for {task_id}.")

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
    # Print available OpenAI models for this API key
    # openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # print("🔍 Fetching available OpenAI models for your API key...")
    # try:
    #     models = openai_client.models.list()
    #     print("✅ Available OpenAI models:")
    #     for m in models.data:
    #         print("   -", m.id)
    # except Exception as e:
    #     print("🚨 Could not fetch models:", e)

    llm = ChatOpenAI(model='gpt-4o-mini', api_key=os.getenv("OPENAI_API_KEY"))

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