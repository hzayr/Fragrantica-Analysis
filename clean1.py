# clean name and description columns

import pandas as pd

# Load original file
df = pd.read_csv('fra_perfumes.csv')

# Save copy of original data for comparison
df_before = df.copy()

# add a space before "for" as it's stuck to the previous word (Name column)
df['Name'] = df['Name'].str.replace(r'(?<=[a-zA-Z])for', ' for', regex=True)

# Add a space before and after "by" only if it's stuck to both the previous and next word
df['Description'] = df['Description'].str.replace(r'(?<!\s)by(?!\s)', ' by ', regex=True)

# Add a space before "is" only if it's stuck to the previous word and not already followed by a space or part of another word
df['Description'] = df['Description'].str.replace(r'(?<=[a-zA-Z])(?<!\b)is(?!\w)', ' is', regex=True)

#####################

# Compare changes - Name and Description
cols_to_check = ['Name', 'Description']
mask = (df[cols_to_check] != df_before[cols_to_check]).any(axis=1)

# Combine before/after for just those columns
df_diff = pd.concat(
    [df_before.loc[mask, cols_to_check], df.loc[mask, cols_to_check]],
    axis=1,
    keys=['Before', 'After']
)

# Print differences
print(df_diff.head(10))

# Save the cleaned DataFrame to a new CSV file
# df.to_csv('clean1.csv', index=False)


