import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv

load_dotenv()
print("Loaded API key:", os.getenv("OPENAI_API_KEY"))

from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm import ChatOpenAI


async def main():
	browser_session = BrowserSession(
		browser_profile=BrowserProfile(
			keep_alive=True,
			user_data_dir=None,
			headless=False,
		)
	)
	await browser_session.start()

	current_agent = None
	llm = ChatOpenAI(model='gpt-4.1-mini', api_key=os.getenv("OPENAI_API_KEY"))

	task1 = """
	Go to lidl's germany website, search for the following products in GERMAN:
	- clementine


	
	For each product, find: 
	- Product Name (from the list above in english)
	- Website Name of the product (in whatever language the website is in)
	- Price per kg
	- Price per unit
	- Supermarket Name
	- Country

	Guidelines:
	- You may find product with price per unit, or others with price per kg, or both. Fill out which ever field you find. 
	- Please avoid prices in discounts, we want the regular price.
	- Please extract bio/organic/ecological products, if available from the same category. e.g. get BIO apple AND normal apple and extract all info for BOTH whenever there is BIO options. 
	- If you find a product that is not in the list, please ignore it.
	- Return a JSON with ALL the products found, with the following structure:
	```json
	{
		"products": [
			{
				"name": "avocado",
				"website_name": "Avocado Bio 400g",
				"price_per_kg": "3.99",
				"price_per_unit": "0.99",
				"supermarket_name": "Lidl"
				"country": "Germany"
				"bio": true
			},
			{
				"name": "clementine",
				"website_name": "Clementine 500g",
				"price_per_kg": null,
				"price_per_unit": "1.29",
				"supermarket_name": "Lidl",
				"country": "Germany"
				"bio": false
			},
			...
		]
	}
	```
	"""

	agent1 = Agent(
		task=task1,
		browser_session=browser_session,
		llm=llm,
		output_dir="output",
		product= "avocado"
	)

	await asyncio.gather(agent1.run())
	await browser_session.kill()


asyncio.run(main())
