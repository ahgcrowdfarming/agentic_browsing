import asyncio
import os
import sys
import tempfile
import json
import time
from typing import List, Optional
from urllib.parse import urlparse
import datetime
from datetime import date
import re
import glob

# --- Setup and Imports ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm import ChatOpenAI
from browser_use.llm.exceptions import ModelProviderError
from network_strategy import base_browser_args 
from openai import OpenAI

# Import Agent Prompts and Supermarket/Country lists
from prompts import PROMPT_FIRST_SUPERMARKET_PRODUCT, PROMPT_REST_SUPERMARKET_PRODUCTS
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
    scrapped_date: Optional[str] = None
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

    # date normalization: expect YYYY-MM-DD
    date_str = product.get("scrapped_date") or date.today().strftime("%Y-%m-%d")
    try:
        y, m, d = re.split(r"[\/\-\.]", date_str)[:3]
    except Exception:
        y, m, d = date.today().strftime("%Y"), date.today().strftime("%m"), date.today().strftime("%d")

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


def add_partition_columns(data_dict: dict) -> dict:
    """Add year, month, day partition columns to the dictionary for Foundry compatibility.
    These are inferred from scrapped_date and added only during serialization."""
    result = data_dict.copy()
    
    # Get products list and ensure it exists
    products = result.get('products', [])
    if not products:
        return result
        
    # Add partition columns to each product
    for product in products:
        if 'scrapped_date' in product:
            try:
                # Parse date components from scrapped_date (YYYY-MM-DD format)
                y, m, d = product['scrapped_date'].split('-')
                # Add partition columns as separate fields
                product['year'] = int(y)
                product['month'] = int(m)
                product['day'] = int(d)
            except (ValueError, AttributeError):
                # If parsing fails, use today's date as fallback
                today = date.today()
                product['year'] = today.year
                product['month'] = today.month
                product['day'] = today.day
    
    return result


def _clean_chrome_session_files(user_data_dir: str) -> list:
    """Safely remove Chromium session/tab files that cause Chrome to restore previous tabs.
    This intentionally avoids deleting cookies or preferences. It targets filenames
    like 'Current Session', 'Current Tabs', 'Last Session', 'Last Tabs', and
    similar session/tab dumps found under profile directories (e.g. 'Default').

    Returns list of removed file paths.
    """
    removed = []
    try:
        # Walk the user_data_dir looking for session/tab files in any profile subfolder
        for root, dirs, files in os.walk(user_data_dir):
            for fname in files:
                lname = fname.lower()
                # Target common Chromium session/tab filenames
                if (
                    'current session' in lname
                    or 'current tabs' in lname
                    or 'last session' in lname
                    or 'last tabs' in lname
                    or re.match(r'session_\d+', lname)
                    or re.match(r'tabs_\d+', lname)
                ):
                    path = os.path.join(root, fname)
                    try:
                        os.remove(path)
                        removed.append(path)
                    except Exception:
                        # best-effort; continue
                        print(f"⚠️ [Browser] Could not remove session file {path}")
    except Exception as e:
        print(f"⚠️ [Browser] Error while cleaning session files: {e}")
    return removed

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
                today = date.today()
                for product in final_data.get('products', []):
                    product['scrapped_date'] = today.strftime("%Y-%m-%d")
                    product['_id'] = _build_product_id_from_output(output_path, product)
                
                # Add partition columns just before writing
                final_data_with_partitions = add_partition_columns(final_data)
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(final_data_with_partitions, f, ensure_ascii=False, indent=2)
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
                    today = date.today()
                    for product in final_data.get('products', []):
                        product['scrapped_date'] = today.strftime("%Y-%m-%d")
                        product['_id'] = _build_product_id_from_output(output_path, product)
                    
                    # Add partition columns just before writing
                    final_data_with_partitions = add_partition_columns(final_data)
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(final_data_with_partitions, f, ensure_ascii=False, indent=2)
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
async def process_supermarket_products(country, supermarket, products_list, llm, semaphore, output_dir=OUTPUT_BASE):
    """
    This function handles all products for a single supermarket using one browser session.
    It creates a new agent for each product to maintain LLM freshness.
    """
    async with semaphore:
        supermarket_id = f"{country}_{supermarket}"
        print(f"▶️  [Worker] Starting supermarket session: {supermarket_id}")

        # Quick pre-check: only proceed if there are pending products to process
        supermarket_output_dir = os.path.join(OUTPUT_BASE, country, supermarket)
        pending_products = []
        for product, subtypes in products_list:
            expected_path = os.path.join(supermarket_output_dir, f"{product}.json")
            if not os.path.exists(expected_path):
                pending_products.append((product, subtypes))

        if not pending_products:
            print(f"✅ [Worker] No pending products for {supermarket_id}. Skipping browser startup.")
            return

        browser_session = None
        browser_context = None
        try:
            print(f"🔄 [Browser] Creating shared browser session for {len(pending_products)} products in {supermarket_id}")
            
            # Create a persistent user data directory for this supermarket
            user_data_dir = os.path.join(
                tempfile.gettempdir(),
                "browser_use",
                "profiles",
                f"{country}_{supermarket}"
            )
            os.makedirs(user_data_dir, exist_ok=True)
            # Remove Chromium session/tab dump files to prevent Chrome from
            # restoring previously-open tabs (newsletters, account pages, etc.).
            # This is a best-effort cleanup that avoids touching cookies/preferences.
            try:
                removed = _clean_chrome_session_files(user_data_dir)
                if removed:
                    print(f"🧹 [Browser] Removed {len(removed)} session/tab files to prevent tab restoration: {removed}")
            except Exception as e:
                print(f"⚠️ [Browser] Failed to clean session files: {e}")
            
            # Create and start browser session for this supermarket
            # NOTE: we keep the user_data_dir so cookies/persistent storage survive,
            # but we DO NOT ask the browser to restore previous tabs. Instead we
            # sanitize open tabs after startup and explicitly open the supermarket homepage.
            browser_session = BrowserSession(
                browser_profile=BrowserProfile(
                    headless=False,
                    keep_alive=True,  # Keep the browser process alive between actions
                    user_data_dir=user_data_dir,  # Use persistent profile directory
                    args=[*base_browser_args(country)]
                )
            )

            print(f"📂 [Browser] Using profile dir: {user_data_dir}")
            print(f"DISPLAY={os.environ.get('DISPLAY')}")
            print(f"UA prefix: {(os.environ.get('BROWSER_USER_AGENT') or '')[:50]}")
            
            await browser_session.start()
            print(f"✅ [Browser] Session started for supermarket: {supermarket_id}")

            # Sanitize session: if the current tab is not on the supermarket domain
            # (or is in an unrelated section like 'newsletter'), close all tabs and
            # open the supermarket homepage to ensure a clean starting point.
            try:
                homepage = COUNTRIES[country][supermarket]
                desired_domain = urlparse(homepage).netloc.lower()

                current_page = await browser_session.get_current_page()
                current_url = (getattr(current_page, 'url', '') or '').lower()

                bad_keywords = ('newsletter', '/newsletter', '/account', '/recipes', '/clothes', '/fashion', '/promo')
                needs_reset = False

                if not current_url or desired_domain not in current_url:
                    needs_reset = True
                else:
                    for kw in bad_keywords:
                        if kw in current_url:
                            needs_reset = True
                            break

                if needs_reset:
                    try:
                        print(f"🧹 [Browser] Session URL was unexpected ({current_url}), resetting to homepage {homepage}")
                        # Close all existing pages and open a fresh one
                        bc = getattr(browser_session, 'browser_context', None)
                        if bc:
                            try:
                                pages = list(bc.pages)
                                for p in pages:
                                    try:
                                        if not p.is_closed():
                                            await p.close()
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                            # Create a new page and navigate to homepage
                            try:
                                new_page = await bc.new_page()
                                await new_page.goto(homepage)
                                # Ensure the session object tracks this page
                                browser_session.agent_current_page = new_page
                                browser_session.human_current_page = new_page
                                vp = getattr(browser_session.browser_profile, 'viewport', None)
                                if vp is not None:
                                    try:
                                        await new_page.set_viewport_size(vp)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception as e:
                # Non-fatal: log and continue; agent prompts will still navigate as needed
                print(f"⚠️ [Browser] Session sanitization skipped due to error: {e}")

            # Ensure output directory exists now that we will write files
            os.makedirs(supermarket_output_dir, exist_ok=True)

            # Process each pending product using the same browser session
            for idx, (product, subtypes) in enumerate(pending_products, 1):
                task_id = f"{country}_{supermarket}_{product}"
                output_path = os.path.join(supermarket_output_dir, f"{product}.json")
                print(f"🔄 [Browser] Product {idx}/{len(pending_products)} using shared session")

                try:
                    # Create new agent for each product
                    if idx == 1:
                        # First product: Use full prompt with website navigation
                        prompt = PROMPT_FIRST_SUPERMARKET_PRODUCT.format(
                            website_url=COUNTRIES[country][supermarket],
                            product=product,
                            subtypes=subtypes,
                            country=country,
                            supermarket=supermarket
                        )
                    else:
                        # Subsequent products: Use simplified prompt for current session
                        prompt = PROMPT_REST_SUPERMARKET_PRODUCTS.format(
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
                        output_model_schema=SupermarketOutput,
                        output_dir=agent_artifact_dir,
                        id=task_id,
                        max_steps=40
                    )
                    print(f"🚀 [Worker] Created agent for {task_id}.")

                    await run_agent_with_retry(agent, output_path, llm)
                    print(f"✅ [Worker] Agent finished for {task_id}.")

                    # Optional: Small delay between products to avoid overwhelming the site
                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"💥 [Worker] Error processing product {task_id}: {e}")
                    if not os.path.exists(output_path):
                        with open(output_path, "w", encoding="utf-8") as f:
                            json.dump({"products": []}, f, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"💥 [Worker] Critical error in supermarket session {supermarket_id}: {e}")
        finally:
            # Cleanup: Ensure complete browser session cleanup after all products
            if browser_session:
                try:
                    # First disable keep_alive so stop() will actually close the browser
                    browser_session.browser_profile.keep_alive = False
                    
                    # Try graceful stop first
                    try:
                        await browser_session.stop()
                    except Exception as e:
                        print(f"⚠️ [Browser] Graceful stop failed: {e}")
                    
                    # Then force kill to ensure cleanup
                    try:
                        await browser_session.kill()  # No force param needed
                        print(f"🧹 [Worker] Cleaned up browser session for {supermarket_id}")
                    except Exception as e:
                        print(f"⚠️ [Browser] Kill failed: {e}")
                        # Last resort: direct process kill if we can access it
                        try:
                            proc = getattr(browser_session, '_browser_process', None)
                            if proc:
                                import psutil
                                if psutil.pid_exists(proc.pid):
                                    proc.kill()
                                    print(f"🔨 [Browser] Force-killed browser process {proc.pid}")
                        except Exception:
                            pass
                except Exception as e:
                    print(f"💥 [Worker] Error during browser cleanup: {e}")
            print(f"⏹️  [Worker] Finished supermarket session: {supermarket_id}")

# --- NEW: Task Orchestrator ---
async def main():
    """
    This is the main "Orchestrator". It groups tasks by supermarket and
    processes all products for each supermarket in a single browser session.
    """
    llm = ChatOpenAI(model='gpt-4o-mini-2024-07-18', api_key=os.getenv("OPENAI_API_KEY"))
    semaphore = asyncio.Semaphore(MAX_PARALLEL_BROWSERS)

    # Group jobs by country and supermarket
    supermarket_jobs = {}
    for country, supermarkets in COUNTRIES.items():
        for supermarket in supermarkets:
            key = (country, supermarket)
            supermarket_jobs[key] = []
            for product, subtypes in PRODUCTS.items():
                supermarket_jobs[key].append((product, subtypes))
    
    print(f"Found {len(supermarket_jobs)} supermarkets to process.")
    
    tasks = []
    for (country, supermarket), products_list in supermarket_jobs.items():
        print(f"Scheduling supermarket {supermarket} in {country} with {len(products_list)} products")
        # Quick pre-check: only schedule supermarkets that have pending products
        supermarket_output_dir = os.path.join(OUTPUT_BASE, country, supermarket)
        pending_products = []
        for product, subtypes in products_list:
            expected_path = os.path.join(supermarket_output_dir, f"{product}.json")
            if not os.path.exists(expected_path):
                pending_products.append((product, subtypes))

        if not pending_products:
            print(f"⏭️  Skipping supermarket {supermarket} in {country}: all products already processed.")
            continue

        # Create and schedule the supermarket worker task (only for pending products)
        task = asyncio.create_task(
            process_supermarket_products(country, supermarket, pending_products, llm, semaphore)
        )
        tasks.append(task)

        # Rate Limiting: Wait between starting new supermarket sessions
        await asyncio.sleep(SECONDS_BETWEEN_TASKS)
    
    # Wait for all scheduled tasks to complete
    await asyncio.gather(*tasks)
    print("✅ All supermarkets have been processed. Script finished.")

if __name__ == "__main__":
    asyncio.run(main())