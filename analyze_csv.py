import csv
import json
import pandas as pd

# Analyze the movies CSV file
movies_file = './data/tmdb_5000_movies.csv'
credits_file = './data/tmdb_5000_credits.csv'

print("=== MOVIES CSV FILE ANALYSIS ===")
print(f"File: {movies_file}")

# Read first few lines to understand structure
with open(movies_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    print(f"Number of columns: {len(headers)}")
    print(f"Column names: {headers}")
    
    # Check a few rows to see data types and content
    print("\nSample data (first 3 rows):")
    for i, row in enumerate(reader):
        if i >= 3:
            break
        print(f"Row {i+1}:")
        for j, (header, value) in enumerate(zip(headers, row)):
            print(f"  {header}: {repr(value)[:100]}{'...' if len(repr(value)) > 100 else ''}")
        print()

print("\n" + "="*50)
print("=== CREDITS CSV FILE ANALYSIS ===")
print(f"File: {credits_file}")

# Read first few lines of credits file
with open(credits_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    print(f"Number of columns: {len(headers)}")
    print(f"Column names: {headers}")
    
    print("\nSample data (first 2 rows):")
    for i, row in enumerate(reader):
        if i >= 2:
            break
        print(f"Row {i+1}:")
        for j, (header, value) in enumerate(zip(headers, row)):
            print(f"  {header}: {repr(value)[:100]}{'...' if len(repr(value)) > 100 else ''}")
        print()

# Detailed analysis of specific fields that contain JSON data
print("\n" + "="*50)
print("=== DETAILED FIELD ANALYSIS ===")

movies_df = pd.read_csv(movies_file)
print("\nMovies DataFrame Info:")
print(movies_df.info())
print("\nMovies DataFrame Sample:")
print(movies_df.head(2))

# Check for missing values
print("\nMissing values in Movies CSV:")
print(movies_df.isnull().sum())

# Analyze the JSON fields in movies
print("\nSample of 'genres' field:")
sample_genres = movies_df['genres'].iloc[0]
print(f"Sample value: {sample_genres}")
try:
    parsed_genres = json.loads(sample_genres.replace("'", '"'))
    print(f"Parsed JSON: {parsed_genres}")
except:
    print("Could not parse as JSON directly")

print("\nSample of 'keywords' field:")
sample_keywords = movies_df['keywords'].iloc[0]
print(f"Sample value: {sample_keywords[:200]}...")
try:
    parsed_keywords = json.loads(sample_keywords.replace("'", '"'))
    print(f"First 2 keywords: {parsed_keywords[:2]}")
except:
    print("Could not parse as JSON directly")

print("\nSample of 'production_companies' field:")
sample_prod_comp = movies_df['production_companies'].iloc[0]
print(f"Sample value: {sample_prod_comp[:200]}...")
try:
    parsed_prod_comp = json.loads(sample_prod_comp.replace("'", '"'))
    print(f"First company: {parsed_prod_comp[0] if parsed_prod_comp else 'None'}")
except:
    print("Could not parse as JSON directly")

# Analyze credits file
credits_df = pd.read_csv(credits_file)
print("\nCredits DataFrame Info:")
print(credits_df.info())
print("\nCredits DataFrame Sample:")
print(credits_df.head(1))

print("\nSample of 'cast' field:")
sample_cast = credits_df['cast'].iloc[0]
print(f"Sample value (first 300 chars): {sample_cast[:300]}...")
try:
    parsed_cast = json.loads(sample_cast.replace("'", '"'))
    print(f"First cast member: {parsed_cast[0] if parsed_cast else 'None'}")
except:
    print("Could not parse as JSON directly")

print("\nSample of 'crew' field:")
sample_crew = credits_df['crew'].iloc[0]
print(f"Sample value (first 300 chars): {sample_crew[:300]}...")
try:
    parsed_crew = json.loads(sample_crew.replace("'", '"'))
    print(f"First crew member: {parsed_crew[0] if parsed_crew else 'None'}")
except:
    print("Could not parse as JSON directly")

