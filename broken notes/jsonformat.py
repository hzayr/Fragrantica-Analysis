import pandas as pd
import ast
import json

# Load the CSV
df = pd.read_csv('perfumes_table.csv')

# Convert notes from string to list and then back to JSON-like string
def fix_notes(val):
    if pd.isna(val):
        return val
    try:
        parsed = ast.literal_eval(val)  # Converts string to Python list
        return json.dumps(parsed, ensure_ascii=False)  # Converts to JSON string with double quotes
    except:
        return val  # Leave unchanged if parsing fails

df['notes'] = df['notes'].apply(fix_notes)

# Save the cleaned file
df.to_csv('cleaned.csv', index=False)

print("Fixed 'notes' column and saved to 'cleaned.csv'")
