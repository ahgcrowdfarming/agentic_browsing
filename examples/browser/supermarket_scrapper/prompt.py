PROMPT_TEMPLATE = """
Your task: extract the **price per kilogram (price_per_kg)** of **{product}** from **{website_url}**, using the exact logic below. 

---

## STEP 1: Access Website
1. Open **{website_url}**.  
2. **If asked for postal code**, use a **central {country} code** (e.g., Germany → `10115`). If not asked, ignore.
3. **If asked to pick a store** within that postocode, choose the **first option**.  
4. Wait until prices fully load.

---
## STEP 2: Find Product and collect Info
- Translate **{product}** to the local {country} language.
- Search for **translated {product} name** using the {country} language.
- Within the search results view, try to find the price per kilo for atleast 3-4 products. SPECIAL INSTRUCTIONS ON HOW TO FIND THE PRICE PER KILO BELOW:

**1️⃣ If you find the direct per-kg price:**  
If shown (e.g., “3,99 €/kg”), record value in `"price_per_kg"`.

**2️⃣ If you find the derived per-kg price (from weight):**  
If shown as “2,99 €/750g”:  
- Extract total price and weight.  
- Convert to kg → compute `price_per_kg = total_price / weight_kg`.  

**3️⃣ If you find other formats of price:**  
If sold per piece or pack (e.g., “1 mango 2€”):  
- Record unit price.  
- Estimate weight from general knowledge (e.g., mango ≈ 0.4 kg).  
- Compute `price_per_kg = price_per_unit / estimated_weight`.  
- Add `"estimation_notes"` (e.g., “1 mango ≈ 0.4 kg”).
---

## STEP 3: Output Data Formatting 
- For every product item found, create a new dict within the products list.
- Some info on the product may be missing, that's ok. Just leave as null or omit.
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