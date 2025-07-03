import pandas as pd

def is_empty_array(val):
    if pd.isna(val):
        return True
    val = str(val).strip()
    return val in ('{}', '[]', '')

def parse_notes_to_pg_array(text):
    if pd.isna(text) or text == '':
        return '{}'
    # Split by comma and strip whitespace
    notes_list = [n.strip() for n in text.split(',')]
    # Escape commas or braces inside notes if needed here
    # Join with commas inside curly braces to match PostgreSQL array literal syntax
    return '{' + ','.join(notes_list) + '}'

def main():
    print("Loading CSV files...")
    cleaned_df = pd.read_csv('cleaned.csv')
    supabase_df = pd.read_csv('supabase.csv')

    # Normalize URLs (strip whitespace)
    cleaned_df['url'] = cleaned_df['url'].astype(str).str.strip()
    supabase_df['url'] = supabase_df['url'].astype(str).str.strip()

    print("Filtering rows with empty Top, Middle, and Base Notes...")
    mask_empty_notes = (
        supabase_df['Top Notes'].apply(is_empty_array) &
        supabase_df['Middle Notes'].apply(is_empty_array) &
        supabase_df['Base Notes'].apply(is_empty_array)
    )
    supabase_empty = supabase_df[mask_empty_notes]

    print(f"Found {len(supabase_empty)} rows with empty notes.")

    # Prepare cleaned_df for merging: only need url and notes (to map to Notes)
    cleaned_notes_df = cleaned_df[['url', 'notes']].copy()
    cleaned_notes_df.rename(columns={'notes': 'Notes_cleaned'}, inplace=True)

    print("Merging with cleaned notes on url...")
    merged = supabase_empty.merge(
        cleaned_notes_df,
        on='url',
        how='left'
    )

    # Update Notes column if Notes_cleaned exists
    def update_notes(row):
        if pd.notna(row['Notes_cleaned']) and row['Notes_cleaned'] != '':
            return parse_notes_to_pg_array(row['Notes_cleaned'])
        else:
            return row['Notes']

    print("Updating Notes column with cleaned notes where available...")
    merged['Notes'] = merged.apply(update_notes, axis=1)

    # Replace updated rows back into supabase_df
    supabase_df.loc[mask_empty_notes, 'Notes'] = merged['Notes'].values

    print("Saving updated supabase data to supabase_updated.csv...")
    supabase_df.to_csv('supabase_updated.csv', index=False)
    print("Done!")

if __name__ == "__main__":
    main()
