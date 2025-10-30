PROMPT_FIRST_SUPERMARKET_PRODUCT = """
Your task: extract the **price per kilogram (price_per_kg)** of **{product}** from **{website_url}**, using the exact logic below. 

---

## STEP 1: Access Website
1. Open **{website_url}**.  
2. **If asked for postal code** or find yourself in a map to pick a location, use a **central {country} code or location** (e.g., Germany → `10115`). If not asked, ignore.
3. **If asked to pick a store** within that postocode, choose the **first option**.  
4. If you get rejected by a cookie or age verification popup, or any other popup, **bypass it** by clicking the appropriate button (e.g., "Accept All", "Yes, I am over 18", "Close", etc.).
5. If you get blocked by a CAPTCHA, Cloudflare verification or simply got blocked from the site, **stop immediately** and return an empty list of products.
6. If you manage to access, wait until prices fully load.

---
## STEP 2: Find Product and collect Info
- Translate **{product}** to the local {country} language.
- Search for **translated {product} name** using the {country} language.
- !Important! You are ONLY looking for the **fresh/raw** version of {product}. Please do not return data for dried {product}, {product} juice, {product} smoothie, {product} sauce, {product} jam etc. 
- If at a first glance you can't find any that apply, please just return an empty list of products. Given it may not be {product} season, or the supermarket may not sell it. 
- If there are applicable candidates Within the search results view, try to find the price per kilo for atleast 3-4 {product} products. 
SPECIAL INSTRUCTIONS ON HOW TO FIND THE PRICE PER KILO BELOW:

**1️⃣ If you find the direct per-kg price:**  
If shown (e.g., "3,99 €/kg"), record value in `"price_per_kg"`.

**2️⃣ If you find the derived per-kg price (from weight):**  
If shown as "2,99 €/750g":  
- Extract total price and weight.  
- Convert to kg → compute `price_per_kg = total_price / weight_kg`.  

**3️⃣ If you find other formats of price:**  
If sold per piece or pack (e.g., "1 mango 2€"):  
- Record unit price.  
- Estimate weight from general knowledge (e.g., mango ≈ 0.4 kg).  
- Compute `price_per_kg = price_per_unit / estimated_weight`.  
- Add `"estimation_notes"` (e.g., "1 mango ≈ 0.4 kg").
---

## STEP 3: Output Data Formatting 
- For every product item found, create a new dict within the products list.
- Some info on the product may be missing, that's ok. Just leave as null or omit.
- Pick one subtype from the list of subtypes below that best matches the product found. Options are: {subtypes}
- If no products found, return an empty list of products inside the JSON object.
- Output **ONE JSON object** only, concatenating all individual product JSONs, **no file edits**
- Do NOT write to any file; just return the JSON object as your final output.
- Call `done` once.  
- Output a JSON object with the following structure:
```json
   {{
     "products": [
       {{
            "name": "{product}", 
            "subtype": "{subtypes[0]}",
            "website_product_name": "Mango de Avión",
            "price_per_kg": 5.0,
            "price_per_unit": 2.0,
            "currency": "EUR",
            "original_price_info": "2,00 € / pieza",
            "estimation_notes": "Estimated weight for 1 mango is 0.4kg.",
            "supermarket_name": "{supermarket}",
            "country": "{country}",
            "bio": false,
       }},
     ]
   }}
"""

PROMPT_REST_SUPERMARKET_PRODUCTS = """
Your task: extract the **price per kilogram (price_per_kg)** of **{product}** from the current {supermarket} webpage in {country}. You are already on the correct website with all permissions and cookies accepted.

---

## STEP 1: Search for Product
- You are already on the {supermarket} website with all permissions set.
- Translate **{product}** to the local {country} language if needed.
- Use the search bar or navigation menu to find **{product}**.
- DO NOT try to navigate to the homepage or re-accept cookies - you are already on the correct website.
- If you get blocked by a CAPTCHA or verification, stop immediately and return an empty list.

---
## STEP 2: Find Product and collect Info
- !Important! You are ONLY looking for the **fresh/raw** version of {product}. Do not return data for processed versions.
- In the search results, locate fresh {product} items and find prices for 3-4 products if available.
- If you can't find any applicable items, return an empty list.

Additional navigation guidance:
- Before you search, make sure you are in the supermarket's main shopping area (product listings, categories or search results). If the site has taken you to a non-shopping subsection (for example: newsletter, clothing, recipes, account, or promotional pages), return to the supermarket's homepage or main shopping section and then perform the search. Do not spend time exploring unrelated sections; if you cannot reach the main shopping area, return an empty list of products.

Price Collection Instructions:
**1️⃣ Direct per-kg price:** Use if shown (e.g., "3,99 €/kg")
**2️⃣ Derived price:** Convert if shown as (e.g., "2,99 €/750g")
**3️⃣ Unit price:** For items sold per piece:
  - Record unit price
  - Estimate typical weight
  - Calculate price per kg
  - Add estimation notes

---
## STEP 3: Output Data
Return a JSON object with the same structure as before:
```json
   {{
     "products": [
       {{
            "name": "{product}", 
            "subtype": "{subtypes[0]}",
            "website_product_name": "Example Name",
            "price_per_kg": 5.0,
            "price_per_unit": 2.0,
            "currency": "EUR",
            "original_price_info": "2,00 € / unit",
            "estimation_notes": "Weight estimation if needed",
            "supermarket_name": "{supermarket}",
            "country": "{country}",
            "bio": false,
       }},
     ]
   }}
```

Remember:
- Pick appropriate subtype from: {subtypes}
- Include estimation notes if weight was estimated
- Return empty product list if nothing found
- Call `done` when finished
"""