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
    estimated_price_per_kg: Optional[float] = None
    currency: Optional[str] = None
    original_price_info: Optional[str] = None
    estimation_notes: Optional[str] = None
    supermarket_name: str
    country: str
    bio: bool

class SupermarketOutput(BaseModel):
    products: List[Product]

# --- CORE CONFIGURATION ---
# List of all products to search for.
PRODUCTS = [
    "aubergine", "mango", "pomegranate", "banana", "clementine", "orange", "avocado",
    "mandarin", "blood orange", "lemon", "extra virgin olive oil", "kiwi", "persimmon"
]

# Dictionary of countries and their supermarkets.
COUNTRIES = {
    "Germany": {

    },
    "France": {
        "FranceAgriMer": "https://rnm.franceagrimer.fr/prix?MARCHES&FRUITS-ET-LEGUMES"
    },
    
    "UK": {
    },
    
    "Spain": {
        "MercaMadrid":  "https://www.mercamadrid.es",
        "Mercabarna": "https://www.mercabarna.cat/es/",
        "Mercasa": "https://www.mercasa.es",
        "Ministerio de Agricultura": "https://www.mapa.gob.es/es/agricultura/temas/producciones-agricolas/frutas-y-hortalizas/boletines_2025" #Boletin semanal
    }
}

# --- PROMPT (unchanged) ---
# --- [NEW] PROMPT for Aggregators ---
PROMPT_TEMPLATE = """
You are a data analyst. Your task is to find the most recent official price data for the product "{product}" from a national publication or data aggregator.

**Primary Objective: Find the MOST RECENT Price**
1.  Your first step is to go directly to this URL: {website_url}
2.  This is a complex data website, not a simple online store. You must navigate the site to find the latest price information. Look for sections like "Market Data", "Prices" (Preise, Prix, Precios), "Statistics", or "Publications".
3.  Your goal is to find the **most recent report or data entry**. Prioritize data from the current month or week of **August 2025**. You must identify the date of the price publication.
4.  The price data is likely in a data table, a chart, or a downloadable report (like a PDF or Excel file). You must analyze these to find the price for "{product}".

**Product Selection Criteria:**
- Find the most **raw or primary form** of the product. For example, if the report lists different categories like "Avocado, Cat. I, Origin: Peru", that is the correct entry.
- IGNORE processed products like juices, spreads, or oils (unless "olive oil" is the target product).
- Look for distinctions between **conventional** and **organic** ('bio', 'ökologisch', 'biologique') products and create separate entries if you find them.

**Data Normalization and Recording:**
- The price will most likely be per kg. If so, extract it directly into the `price_per_kg` field.
- If the price is per tonne, you must calculate the price per kg by dividing by 1000.
- Use the `original_price_info` field to note the date of the report you found (e.g., "Price from weekly report 04/08/2025").

**CRITICAL INSTRUCTIONS FOR FINAL OUTPUT:**
- Call the `done` action after you have found the most recent data.
- In the `text` argument of the `done` action, provide ONLY a single JSON object.
- **IMPORTANT**: Use the `supermarket_name` field to store the name of the **data source itself** (e.g., "{supermarket}").
- The JSON object must strictly follow this structure:
   ```json
   {{
     "products": [
       {{
         "name": "avocado",
         "website_product_name": "Avocado, Cat. I, Origin: Peru",
         "price_per_kg": 2.75,
         "price_per_unit": null,
         "estimated_price_per_kg": null,
         "currency": "EUR",
         "original_price_info": "Price from weekly bulletin for 2025-08-04",
         "estimation_notes": null,
         "supermarket_name": "MercaMadrid",
         "country": "{country}",
         "bio": false
       }}
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
            # 2. Isolated Browser Session for this task
            browser_session = BrowserSession(
                browser_profile=BrowserProfile(
                    # Headless is better for long, unsupervised runs
                    headless=False,
                    # Keep user_data_dir=None for clean sessions
                    user_data_dir=None
                )
            )
            await browser_session.start()

            # 3. Create and run the agent
            prompt = PROMPT_TEMPLATE.format(
                website_url=COUNTRIES[country][supermarket],
                product=product,
                country=country,
                supermarket=supermarket
            )
            agent_artifact_dir = os.path.join(tempfile.gettempdir(), "browser_use", task_id)

            agent = Agent(
                task=prompt,
                browser_session=browser_session,
                llm=llm,
                output_model_schema=SupermarketOutput, # This was missing from your Agent() call before
                output_dir=agent_artifact_dir,
                id=task_id
            )

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