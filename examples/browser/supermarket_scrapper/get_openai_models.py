from openai import OpenAI
from dotenv import load_dotenv
import os
from pathlib import Path

# Load environment variables (expects OPENAI_API_KEY in .env)
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Fetch models
models = [m.id for m in client.models.list().data]

# Define output file path (same folder as this script)
output_path = Path(__file__).parent / "available_openai_models.py"

# Write models to the file as a Python list
with open(output_path, "w") as f:
    f.write("# Auto-generated list of available OpenAI models\n")
    f.write("available_models = [\n")
    for model in models:
        f.write(f"    '{model}',\n")
    f.write("]\n")

print(f"✅ Wrote {len(models)} models to {output_path}")
