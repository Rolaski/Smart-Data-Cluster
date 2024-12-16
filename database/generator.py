import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv()

# Dane połączenia do bazy danych PostgreSQL
db_config = {
    'database': 'SDC-db',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


# Funkcja do utworzenia tabeli w PostgreSQL
def create_tables(cursor):
    # Tabela dla pliku Groceries data.csv
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS groceries_data (
            id SERIAL PRIMARY KEY,
            Member_number BIGINT,
            Date DATE,
            itemDescription VARCHAR(255),
            year INT,
            month INT,
            day INT,
            day_of_week VARCHAR(50)
        );
    """)

    # Tabela dla pliku basket.csv
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS basket (
            id SERIAL PRIMARY KEY,
            col_0 VARCHAR(255),
            col_1 VARCHAR(255),
            col_2 VARCHAR(255),
            col_3 VARCHAR(255),
            col_4 VARCHAR(255),
            col_5 VARCHAR(255),
            col_6 VARCHAR(255),
            col_7 VARCHAR(255),
            col_8 VARCHAR(255),
            col_9 VARCHAR(255)
        );
    """)


# Funkcja do wypełnienia tabeli danymi z CSV
def populate_table_from_csv(cursor, table_name, csv_file, columns, rename_columns=None):
    # Wczytaj plik CSV
    df = pd.read_csv(csv_file)

    # Zmień nazwy kolumn w DataFrame, jeśli podano mapowanie
    if rename_columns:
        df.rename(columns=rename_columns, inplace=True)

    # Zamień NaN na None (NULL) w DataFrame
    df = df.where(pd.notnull(df), None)

    # Iteracja po wierszach DataFrame
    for _, row in df.iterrows():
        # Budowanie zapytania SQL
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Konwersja wiersza na tuple dla zapytania
        values = tuple(row[col] for col in columns)

        # Wykonanie zapytania SQL
        cursor.execute(query, values)



# Główna funkcja
def main():
    connection = None
    cursor = None
    try:
        # Połącz z bazą danych
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Utwórz tabele
        create_tables(cursor)

        # Wypełnij tabelę groceries_data
        groceries_columns = ['Member_number', 'Date', 'itemDescription', 'year', 'month', 'day', 'day_of_week']
        populate_table_from_csv(cursor, 'groceries_data', 'Groceries data.csv', groceries_columns)

        # Mapowanie kolumn z 0,1,2,... na col_0, col_1,...
        rename_columns = {str(i): f"col_{i}" for i in range(10)}
        basket_columns = ['col_0', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 'col_7', 'col_8', 'col_9']
        populate_table_from_csv(cursor, 'basket', 'basket.csv', basket_columns, rename_columns)

        # Zatwierdź zmiany w bazie danych
        connection.commit()
        print("Data has been successfully imported into the database.")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # Zamknij połączenie, jeśli zostało otwarte
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
