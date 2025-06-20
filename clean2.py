# remove null values in name and add id column (primary key)

import pandas as pd

# Read the CSV file
df = pd.read_csv('clean1.csv')

# Drop rows where 'Name' is null
df = df.dropna(subset=['Name'])

# Add an 'id' column starting from 1
df['id'] = range(1, len(df) + 1)

# Move 'id' column to the front
cols = ['id'] + [col for col in df.columns if col != 'id']
df = df[cols]

# Save to a new CSV file
df.to_csv('clean2.csv', index=False)

print("Operation Successful")
