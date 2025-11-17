import sqlite3
import json
import pandas as pd
from typing import List, Dict, Any

def create_database_schema(db_path: str) -> sqlite3.Connection:
    """Create the SQLite database with proper schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create movies table
    cursor.execute('''
        CREATE TABLE movies (
            id INTEGER PRIMARY KEY,
            budget INTEGER,
            homepage TEXT,
            original_language TEXT,
            original_title TEXT,
            overview TEXT,
            popularity REAL,
            release_date TEXT,
            revenue INTEGER,
            runtime REAL,
            status TEXT,
            tagline TEXT,
            title TEXT,
            vote_average REAL,
            vote_count INTEGER
        )
    ''')
    
    # Create genres table
    cursor.execute('''
        CREATE TABLE genres (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    ''')
    
    # Create keywords table
    cursor.execute('''
        CREATE TABLE keywords (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    ''')
    
    # Create production_companies table
    cursor.execute('''
        CREATE TABLE production_companies (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    # Create production_countries table
    cursor.execute('''
        CREATE TABLE production_countries (
            iso_3166_1 TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    # Create spoken_languages table
    cursor.execute('''
        CREATE TABLE spoken_languages (
            iso_639_1 TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')
    
    # Create people table (for cast and crew)
    cursor.execute('''
        CREATE TABLE people (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            gender INTEGER  -- 0: Unspecified, 1: Female, 2: Male
        )
    ''')
    
    # Create junction tables
    cursor.execute('''
        CREATE TABLE movies_genres (
            movie_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (genre_id) REFERENCES genres (id),
            PRIMARY KEY (movie_id, genre_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_keywords (
            movie_id INTEGER,
            keyword_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (keyword_id) REFERENCES keywords (id),
            PRIMARY KEY (movie_id, keyword_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_production_companies (
            movie_id INTEGER,
            company_id INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (company_id) REFERENCES production_companies (id),
            PRIMARY KEY (movie_id, company_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_production_countries (
            movie_id INTEGER,
            country_iso TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (country_iso) REFERENCES production_countries (iso_3166_1),
            PRIMARY KEY (movie_id, country_iso)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_spoken_languages (
            movie_id INTEGER,
            language_iso TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (language_iso) REFERENCES spoken_languages (iso_639_1),
            PRIMARY KEY (movie_id, language_iso)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_cast (
            movie_id INTEGER,
            person_id INTEGER,
            cast_id INTEGER,
            character TEXT,
            credit_id TEXT,
            gender INTEGER,
            order_num INTEGER,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (person_id) REFERENCES people (id),
            PRIMARY KEY (movie_id, person_id, cast_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE movies_crew (
            movie_id INTEGER,
            person_id INTEGER,
            credit_id TEXT,
            department TEXT,
            gender INTEGER,
            job TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (id),
            FOREIGN KEY (person_id) REFERENCES people (id),
            PRIMARY KEY (movie_id, person_id, job, department)
        )
    ''')
    
    conn.commit()
    return conn

def parse_json_field(json_str: str) -> List[Dict[str, Any]]:
    """Parse JSON string field and return list of dictionaries"""
    if not json_str or json_str == '[]':
        return []
    
    # Handle the case where the JSON uses single quotes instead of double quotes
    json_str = json_str.replace("'", '"')
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # If direct parsing fails, try fixing common issues
        try:
            # Replace escaped unicode that might be causing issues
            json_str = json_str.encode('utf-8').decode('unicode_escape')
            return json.loads(json_str)
        except:
            print(f"Error parsing JSON: {json_str[:100]}...")
            return []

def insert_movies_data(conn: sqlite3.Connection, movies_df: pd.DataFrame):
    """Insert movies data into the database"""
    cursor = conn.cursor()
    
    for _, row in movies_df.iterrows():
        # Prepare the movie data
        movie_data = (
            row['id'], row['budget'], row['homepage'], row['original_language'],
            row['original_title'], row['overview'], row['popularity'], 
            row['release_date'], row['revenue'], row['runtime'], 
            row['status'], row['tagline'], row['title'], 
            row['vote_average'], row['vote_count']
        )
        
        cursor.execute('''
            INSERT OR REPLACE INTO movies 
            (id, budget, homepage, original_language, original_title, 
             overview, popularity, release_date, revenue, runtime, 
             status, tagline, title, vote_average, vote_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', movie_data)
    
    conn.commit()

def insert_related_data(conn: sqlite3.Connection, movies_df: pd.DataFrame):
    """Insert related data (genres, keywords, production companies, etc.)"""
    cursor = conn.cursor()
    
    # Process each movie to extract and insert related data
    for _, row in movies_df.iterrows():
        movie_id = row['id']
        
        # Process genres
        genres = parse_json_field(row['genres'])
        for genre in genres:
            cursor.execute('INSERT OR IGNORE INTO genres (id, name) VALUES (?, ?)', 
                          (genre['id'], genre['name']))
            cursor.execute('INSERT OR IGNORE INTO movies_genres (movie_id, genre_id) VALUES (?, ?)', 
                          (movie_id, genre['id']))
        
        # Process keywords
        keywords = parse_json_field(row['keywords'])
        for keyword in keywords:
            cursor.execute('INSERT OR IGNORE INTO keywords (id, name) VALUES (?, ?)', 
                          (keyword['id'], keyword['name']))
            cursor.execute('INSERT OR IGNORE INTO movies_keywords (movie_id, keyword_id) VALUES (?, ?)', 
                          (movie_id, keyword['id']))
        
        # Process production companies
        companies = parse_json_field(row['production_companies'])
        for company in companies:
            cursor.execute('INSERT OR IGNORE INTO production_companies (id, name) VALUES (?, ?)', 
                          (company['id'], company['name']))
            cursor.execute('INSERT OR IGNORE INTO movies_production_companies (movie_id, company_id) VALUES (?, ?)', 
                          (movie_id, company['id']))
        
        # Process production countries
        countries = parse_json_field(row['production_countries'])
        for country in countries:
            cursor.execute('INSERT OR IGNORE INTO production_countries (iso_3166_1, name) VALUES (?, ?)', 
                          (country['iso_3166_1'], country['name']))
            cursor.execute('INSERT OR IGNORE INTO movies_production_countries (movie_id, country_iso) VALUES (?, ?)', 
                          (movie_id, country['iso_3166_1']))
        
        # Process spoken languages
        languages = parse_json_field(row['spoken_languages'])
        for language in languages:
            cursor.execute('INSERT OR IGNORE INTO spoken_languages (iso_639_1, name) VALUES (?, ?)', 
                          (language['iso_639_1'], language['name']))
            cursor.execute('INSERT OR IGNORE INTO movies_spoken_languages (movie_id, language_iso) VALUES (?, ?)', 
                          (movie_id, language['iso_639_1']))
        
        # Commit every 100 movies to avoid memory issues
        if _ % 100 == 0:
            conn.commit()
    
    conn.commit()

def insert_credits_data(conn: sqlite3.Connection, credits_df: pd.DataFrame):
    """Insert credits data (cast and crew) into the database"""
    cursor = conn.cursor()
    
    for _, row in credits_df.iterrows():
        movie_id = row['movie_id']
        
        # Process cast
        cast = parse_json_field(row['cast'])
        for person in cast:
            # Insert person into people table
            cursor.execute('INSERT OR IGNORE INTO people (id, name, gender) VALUES (?, ?, ?)', 
                          (person['id'], person['name'], person.get('gender', 0)))
            
            # Insert cast relationship
            cursor.execute('''
                INSERT OR REPLACE INTO movies_cast 
                (movie_id, person_id, cast_id, character, credit_id, gender, order_num)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                movie_id, person['id'], person['cast_id'], person['character'], 
                person['credit_id'], person.get('gender', 0), person['order']
            ))
        
        # Process crew
        crew = parse_json_field(row['crew'])
        for person in crew:
            # Insert person into people table (if not already there)
            cursor.execute('INSERT OR IGNORE INTO people (id, name, gender) VALUES (?, ?, ?)', 
                          (person['id'], person['name'], person.get('gender', 0)))
            
            # Insert crew relationship
            cursor.execute('''
                INSERT OR REPLACE INTO movies_crew 
                (movie_id, person_id, credit_id, department, gender, job)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                movie_id, person['id'], person['credit_id'], 
                person['department'], person.get('gender', 0), person['job']
            ))
        
        # Commit every 100 movies to avoid memory issues
        if _ % 100 == 0:
            conn.commit()
    
    conn.commit()

def main():
    # Define paths
    movies_csv_path = '/home/chx/mprojects/Movie-Backend-api/data/tmdb_5000_movies.csv'
    credits_csv_path = '/home/chx/mprojects/Movie-Backend-api/data/tmdb_5000_credits.csv'
    db_path = '/home/chx/mprojects/Movie-Backend-api/movies.db'
    
    print("Creating database schema...")
    conn = create_database_schema(db_path)
    
    print("Loading movies CSV data...")
    movies_df = pd.read_csv(movies_csv_path)
    
    print("Loading credits CSV data...")
    credits_df = pd.read_csv(credits_csv_path)
    
    print("Inserting movies data...")
    insert_movies_data(conn, movies_df)
    
    print("Inserting related data (genres, keywords, etc.)...")
    insert_related_data(conn, movies_df)
    
    print("Inserting credits data (cast, crew)...")
    insert_credits_data(conn, credits_df)
    
    print("Creating indexes for better performance...")
    cursor = conn.cursor()
    
    # Create indexes to improve query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_release_date ON movies(release_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_vote_average ON movies(vote_average)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_people_name ON people(name)')
    
    conn.commit()
    
    # Print summary statistics
    print("\nDatabase creation completed successfully!")
    print("Summary:")
    cursor.execute('SELECT COUNT(*) FROM movies')
    print(f"- Movies: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM genres')
    print(f"- Genres: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM keywords')
    print(f"- Keywords: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM production_companies')
    print(f"- Production Companies: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM people')
    print(f"- People (actors/crew): {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM movies_cast')
    print(f"- Cast relationships: {cursor.fetchone()[0]}")
    
    cursor.execute('SELECT COUNT(*) FROM movies_crew')
    print(f"- Crew relationships: {cursor.fetchone()[0]}")
    
    conn.close()
    print(f"\nDatabase saved to: {db_path}")

if __name__ == "__main__":
    main()

