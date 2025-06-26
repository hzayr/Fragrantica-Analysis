import pandas as pd
from pydantic import BaseModel, ValidationError, validator
from typing import Optional

# Define validation schema
class Perfume(BaseModel):
    id: int
    Name: str
    Gender: Optional[str]
    Rating_Value: Optional[float]
    Rating_Count: Optional[int]
    Main_Accords: Optional[str]
    Perfumers: Optional[str]
    Description: Optional[str]
    url: Optional[str]

    @validator('Rating_Value')
    def rating_must_be_between_0_and_5(cls, v):
        if v is not None and not (0 <= v <= 5):
            raise ValueError('Rating Value must be between 0 and 5')
        return v

    @validator('Rating_Count')
    def rating_count_must_be_positive(cls, v):
        if v is not None and v < 0:
            raise ValueError('Rating Count must be positive')
        return v

# Step 1: Load CSV
def load_csv(file_path: str) -> pd.DataFrame:
    print(f"Loading data from {file_path}...")
    return pd.read_csv(file_path)

# Step 2: Clean data
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Copy original for comparison
    df_before = df.copy()

    # Clean Name and Description (do not drop rows yet)
    df['Name'] = df['Name'].str.replace(r'(?<=[a-zA-Z])for', ' for', regex=True)
    df['Description'] = df['Description'].str.replace(r'(?<!\s)by(?!\s)', ' by ', regex=True)
    df['Description'] = df['Description'].str.replace(r'(?<=[a-zA-Z])(?<!\b)is(?!\w)', ' is', regex=True)

    # Compare Name/Description before dropping or modifying indexes
    cols_to_check = ['Name', 'Description']
    # Fill missing with empty string to prevent NaN comparison issues
    df_before[cols_to_check] = df_before[cols_to_check].fillna('')
    df[cols_to_check] = df[cols_to_check].fillna('')

    mask = (df_before[cols_to_check] != df[cols_to_check]).any(axis=1)
    print(f"{mask.sum()} rows cleaned in Name/Description")

    # Now drop rows and reset index
    df = df.dropna(subset=['Name']).reset_index(drop=True)

    # Add 'id' column
    df['id'] = range(1, len(df) + 1)
    cols = ['id'] + [col for col in df.columns if col != 'id']
    df = df[cols]

    return df

# Step 3: Validate data
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    valid_rows = []
    errors = []

    for i, row in df.iterrows():
        try:
            perfume = Perfume(
                id=row['id'],
                Name=row['Name'],
                Gender=row.get('Gender', None),
                Rating_Value=row.get('Rating Value', None),
                Rating_Count=row.get('Rating Count', None),
                Main_Accords=row.get('Main Accords', None),
                Perfumers=row.get('Perfumers', None),
                Description=row.get('Description', None),
                url=row.get('url', None),
            )
            valid_rows.append(perfume.dict())
        except ValidationError as e:
            errors.append((i, e.errors()))

    print(f"Validation passed for {len(valid_rows)} rows")
    if errors:
        print(f"Validation errors in {len(errors)} rows")
        for idx, err in errors[:5]:
            print(f"Row {idx}: {err}")

    return pd.DataFrame(valid_rows)

# Step 4: Save cleaned data
def save_clean_data(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    print(f"Cleaned data saved to {path}")

# Main pipeline
def main():
    raw_path = 'fra_perfumes.csv'
    clean_path = 'cleaned_perfumes.csv'

    df = load_csv(raw_path)
    df = clean_data(df)
    df_validated = validate_data(df)
    save_clean_data(df_validated, clean_path)

if __name__ == '__main__':
    main()
