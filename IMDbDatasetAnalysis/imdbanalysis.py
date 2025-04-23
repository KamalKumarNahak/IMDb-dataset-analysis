import pandas as pd
import mysql.connector
import csv
import matplotlib.pyplot as plt
import re
import os

# MySQL configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "jupiter@1234",
    "database": "imdb_db"
}

# 1. Load CSV or JSON data
def load_data(file_path):
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        return pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file format")

# 2. Clean and sanitize data
def clean_data(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=["title", "year", "rating", "language"])
    df["title"] = df["title"].str.strip().str.title()
    df["genre"] = df["genre"].str.replace(r"[^\w\s,]", "", regex=True)
    df["language"] = df["language"].str.strip().str.lower()
    return df

# 3. Upload cleaned data to MySQL
def upload_to_mysql(df, table_name="movies"):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Drop the table if it already exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create the table with 'language' column
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            year INT,
            rating FLOAT,
            genre TEXT,
            language VARCHAR(100)
        )
    """)

    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (title, year, rating, genre, language)
            VALUES (%s, %s, %s, %s, %s)
        """, (row["title"], row["year"], row["rating"], row["genre"], row["language"]))

    conn.commit()
    conn.close()

# 4. Run SQL queries and print analysis
def analyze_data():
    os.makedirs("reports", exist_ok=True)
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    queries = {
        "Top Rated Movies": "SELECT title, rating FROM movies ORDER BY rating DESC LIMIT 10",
        "Average Rating by Year": "SELECT year, AVG(rating) FROM movies GROUP BY year ORDER BY year",
        "Most Common Genres": "SELECT genre, COUNT(*) as count FROM movies GROUP BY genre ORDER BY count DESC LIMIT 10",
        "Top Genres by Average Rating": "SELECT genre, AVG(rating) as avg_rating FROM movies GROUP BY genre ORDER BY avg_rating DESC LIMIT 10"
    }

    for title, query in queries.items():
        print(f"\n📊 {title}")
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        filename = f"reports/{title.replace(' ', '_')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        for row in rows:
            print(row)

    conn.close()

# 5. Interactive console for dynamic queries
def interactive_console():
    print("\n🔎 Welcome to the IMDb Interactive Console!")
    print("Type commands like:\n"
          "• movies from 2012\n"
          "• hindi movies\n"
          "• movies with rating 9\n"
          "• action movies from 2015\n"
          "• tamil action movies\n"
          "• dangal\n"
          "Type 'exit' to quit.")

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    while True:
        query_input = input("\n🎬> ").lower().strip()
        if query_input in ["exit", "quit", "q"]:
            print("👋 Exiting console. Bye!")
            break

        sql = None

        # Match: "movies from 2012"
        match = re.match(r".*from (\d{4})", query_input)
        if match:
            year = match.group(1)
            sql = f"SELECT title, year, rating, genre, language FROM movies WHERE year = {year}"

        # Match: "movies with rating 9"
        match = re.match(r".*rating (?:of )?(\d+(\.\d+)?)", query_input)
        if match:
            rating = match.group(1)
            sql = f"SELECT title, year, rating, genre, language FROM movies WHERE rating >= {rating}"

        # Match: "hindi movies", "action movies", "tamil action movies", etc.
        if not sql:
            words = query_input.split()
            if "movies" in words:
                words.remove("movies")
                lang = genre = None
                for w in words:
                    cursor.execute(f"SELECT DISTINCT language FROM movies WHERE language LIKE '%{w}%'")
                    if cursor.fetchone():
                        lang = w
                    else:
                        genre = w
                if lang and genre:
                    sql = f"SELECT title, year, rating, genre, language FROM movies WHERE genre LIKE '%{genre}%' AND language LIKE '%{lang}%'"
                elif lang:
                    sql = f"SELECT title, year, rating, genre, language FROM movies WHERE language LIKE '%{lang}%'"
                elif genre:
                    sql = f"SELECT title, year, rating, genre, language FROM movies WHERE genre LIKE '%{genre}%'"

        # Match movie title (fallback)
        if not sql:
            sql = f"SELECT title, year, rating, genre, language FROM movies WHERE LOWER(title) LIKE '%{query_input}%'"

        if sql:
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
                if results:
                    print(f"\n🎥 Results ({len(results)}):")
                    for row in results:
                        print(row)
                else:
                    print("❌ No matching movies found.")
            except Exception as e:
                print("⚠️ Error running query:", e)
        else:
            print("❓ Sorry, I couldn't understand that. Try another format.")

    conn.close()

# 6. Main function to run the pipeline
def main():
    df = load_data(r"imdb_large_multilang_100.csv")  # Change path if needed
    cleaned_df = clean_data(df)
    upload_to_mysql(cleaned_df)
    analyze_data()
    interactive_console()

if __name__ == "__main__":
    main()
