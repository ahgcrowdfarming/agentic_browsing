import asyncio
import os
import sys
import tempfile
import json
import time
from typing import List, Optional
import datetime
from datetime import date
import re

# --- Setup and Imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm import ChatOpenAI
from browser_use.llm.exceptions import ModelProviderError
from openai import OpenAI

# Import Agent Prompt Template and Supermarket/Country lists
from prompt import PROMPT_TEMPLATE
from countries import COUNTRIES
from products import PRODUCTS

TOKEN_PRICING = {
    'gpt-4o-mini-2024-07-18': {
        'input': 0.15 / 1_000_000,  # $0.15 per 1 million tokens
        'output': 0.60 / 1_000_000, # $0.60 per 1 million tokens
    }
    # You can add other models here if you use them
}

load_dotenv()
print("Loaded API key:", os.getenv("OPENAI_API_KEY") is not None)

# --- Pydantic Models (unchanged) ---
class Product(BaseModel):
    name: str
    subtype:str
    website_product_name: Optional[str] = None
    price_per_kg: Optional[float] = None
    price_per_unit: Optional[float] = None
    currency: Optional[str] = None
    original_price_info: Optional[str] = None
    estimation_notes: Optional[str] = None
    supermarket_name: str
    country: str
    bio: bool
    # model_used: Optional[str] = None
    # run_total_tokens: Optional[int] = None
    # estimated_cost: Optional[float] = None
    # packaging_type: Optional[str] = None
    # minimum_order_value: Optional[float] = None
    # product_origin: Optional[str] = None
    # product_url: Optional[str] = None
    # is_discounted: Optional[bool] = None
    # discount_details: Optional[str] = None
    # data_scraped_at: Optional[str] = None

class SupermarketOutput(BaseModel):
    products: List[Product]

# --- CORE CONFIGURATION ---

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


def add_agent_cost_to_product_data(final_data, result, llm):
    """
    Calculates the per-product tokens and cost for a run and adds them
    to the extracted product dictionaries.
    """
    try:
        model_name = llm.model
        usage = result.usage
        
        # Get the total token counts from the usage object
        run_total_tokens = usage.total_tokens
        
        # --- REVERT: Manually calculate total cost ---
        # We need the prompt and completion tokens for this.
        total_prompt_tokens = usage.total_prompt_tokens
        total_completion_tokens = usage.total_completion_tokens
        
        run_total_cost = 0.0
        if model_name in TOKEN_PRICING:
            prices = TOKEN_PRICING[model_name]
            input_cost = total_prompt_tokens * prices['input']
            output_cost = total_completion_tokens * prices['output']
            run_total_cost = input_cost + output_cost
        # --- END OF REVERT ---

        # Get the number of products to distribute the cost and tokens
        products = final_data.get('products', [])
        num_products = len(products)
        
        # Calculate per-product values
        tokens_per_product = run_total_tokens / num_products if num_products > 0 else 0
        cost_per_product = run_total_cost / num_products if num_products > 0 else 0
        
        # Assign the simplified, per-product fields
        for product in products:
            product['model_used'] = model_name
            product['tokens_used'] = int(tokens_per_product)
            product['total_cost'] = cost_per_product
            
        print(f"  💸 [Cost] Run Tokens: {run_total_tokens}, Total Cost: ${run_total_cost:.6f}, Products Found: {num_products}")
        
    except Exception as e:
        print(f"🚨 [Cost] Error calculating or adding cost data: {e}")
        
    return final_data

# ID Generator: build _id for each product based on output_path and product fields ---
def _build_product_id_from_output(output_path: str, product: dict) -> str:
    """
    Build id as: country_supermarket_product_priceperkilo_day_month_year
    - priceperkilo uses product['price_per_kg'] if available, formatted as two decimals and dots replaced with underscores
    - date is taken from product['scrapped_date'] if present (DD/MM/YYYY), otherwise today
    """
    # extract country, supermarket, filename from output_path
    parts = os.path.normpath(output_path).split(os.sep)
    country = supermarket = product_fname = "unknown"
    if len(parts) >= 3:
        product_fname = parts[-1]
        supermarket = parts[-2]
        country = parts[-3]
    # product name fallback
    prod_name = product.get("name") or os.path.splitext(product_fname)[0] or "unknown"

    # price_per_kg normalization
    price = product.get("price_per_kg")
    if isinstance(price, (int, float)):
        price_str = f"{price:.2f}"
    else:
        price_str = "unknown"

    price_str = price_str.replace(".", "_")

    # date normalization: expect dd/mm/YYYY
    date_str = product.get("scrapped_date") or date.today().strftime("%d/%m/%Y")
    try:
        d, m, y = re.split(r"[\/\-\.]", date_str)[:3]
    except Exception:
        d, m, y = date.today().strftime("%d"), date.today().strftime("%m"), date.today().strftime("%Y")

    # sanitize parts (lowercase, keep alnum, replace other chars with '-')
    def _sanitize(s: str) -> str:
        s = str(s)
        s = s.strip()
        s = re.sub(r"[^A-Za-z0-9]+", "-", s)
        return s.strip("-").lower() or "unknown"

    id_parts = [
        _sanitize(country),
        _sanitize(supermarket),
        _sanitize(prod_name),
        _sanitize(price_str),
        _sanitize(d),
        _sanitize(m),
        _sanitize(y),
    ]
    return "_".join(id_parts)


# --- Agent Runner (unchanged, it's perfect) ---
async def run_agent_with_retry(agent, output_path, llm, max_retries=1):
    """This function now only contains the retry logic for a single agent run."""
    for attempt in range(max_retries):
        print(f"🔄 [Agent] Attempt {attempt + 1} for {os.path.basename(output_path)}.")
        try:
            result = await agent.run()
            usage = result.usage
            # --- ADD THESE DEBUGGING LINES ---
            print("\n" + "="*20 + " DEBUGGING USAGE OBJECT " + "="*20)
            print(f"Usage Object Type: {type(usage)}")
            print("--- Available Attributes ---")
            print(dir(usage))
            print("="*66 + "\n")
            # --- END OF DEBUGGING LINES ---

            if hasattr(result, "structured_output") and result.structured_output:
                print(f"✅ [Agent] Found structured_output from library for {os.path.basename(output_path)}.")
                final_data = result.structured_output.model_dump()
                final_data = add_agent_cost_to_product_data(final_data, result, llm)
                for product in final_data.get('products', []):
                    product['scrapped_date'] = date.today().strftime("%d/%m/%Y")
                    product['_id'] = _build_product_id_from_output(output_path, product)
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
                    final_data = add_agent_cost_to_product_data(final_data, result, llm)
                    for product in final_data.get('products', []):
                        product['scrapped_date'] = date.today().strftime("%d/%m/%Y")
                        product['_id'] = _build_product_id_from_output(output_path, product)
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
async def process_supermarket_task(country, supermarket, product, subtypes, llm, semaphore, output_dir=OUTPUT_BASE):
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

        # LOCAL TESTING: Skip the checkpointing to force re-runs
        if os.path.exists(output_path):
            print(f"⏭️  [Worker] Skipping {task_id}, output file already exists.")
            return

        browser_session = None
        try:
            
            browser_session = BrowserSession(
                browser_profile=BrowserProfile(
                    # Headless is better for long, unsupervised runs
                    headless=False,
                    user_data_dir=os.environ.get("BROWSER_USER_DATA_DIR") or None,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        # You can add more flags via env without changing code:
                        # They’ll be appended below.
                    ] + (os.environ.get("BROWSER_EXTRA_ARGS", "").split() if os.environ.get("BROWSER_EXTRA_ARGS") else [])
                )
            )
            
            print(f"DISPLAY={os.environ.get('DISPLAY')}")
            print(f"BROWSER_USER_DATA_DIR={os.environ.get('BROWSER_USER_DATA_DIR')}")
            await browser_session.start()
            print(f"✅ [Worker] Browser started for task: {task_id}")

            # 3. Create and run the agent
            prompt = PROMPT_TEMPLATE.format(
                website_url=COUNTRIES[country][supermarket],
                product=product,
                subtypes=subtypes,
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

            await run_agent_with_retry(agent, output_path, llm)
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

    llm = ChatOpenAI(model='gpt-4o-mini-2024-07-18', api_key=os.getenv("OPENAI_API_KEY"))

    semaphore = asyncio.Semaphore(MAX_PARALLEL_BROWSERS)

    # Create a list of all jobs to be done
    all_jobs = []
    for country, supermarkets in COUNTRIES.items():
        for supermarket in supermarkets:
            for product, subtypes in PRODUCTS.items():
                all_jobs.append((country, supermarket, product, subtypes))
    
    print(f"Found {len(all_jobs)} total jobs to process.")
    
    tasks = []
    for job in all_jobs:
        country, supermarket, product, subtypes = job
        
        # Create and schedule the worker task
        task = asyncio.create_task(
            process_supermarket_task(country, supermarket, product, subtypes, llm, semaphore)
        )
        tasks.append(task)
        
        # Rate Limiting: Wait for a few seconds before scheduling the next one
        await asyncio.sleep(SECONDS_BETWEEN_TASKS)
    
    # Wait for all scheduled tasks to complete
    await asyncio.gather(*tasks)
    print("✅ All tasks have been processed. Script finished.")

if __name__ == "__main__":
    asyncio.run(main())