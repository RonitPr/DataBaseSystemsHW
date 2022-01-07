import mysql.connector
from environment import config
import sys


def create_table(cursor, tableName, optionDict):
    # execute on cursor the creation of a table with tableName with the columns described in optionDict
    columns = ""
    for key in optionDict.keys():
        columns += f"{key} {optionDict[key]}, "
    columns = columns[:len(columns) - 2]
    try:
        cursor.execute(f"CREATE TABLE {tableName} ({columns}) ENGINE=InnoDB")
    except Exception as e:
        print(f'Failed Creating {tableName} table in DB with error: {e}.')
    # note - CREATE statements are automatically commited after execution.


def create_database():
    # create connection and cursor to create the tables on.
    con = mysql.connector.connect(**config())
    with con:
        cursor = con.cursor()
        create_table(cursor, 'movie', {
            'movie_id': 'CHAR(10)',
            'title': 'VARCHAR(100) NOT NULL',
            'year': 'YEAR(4) NOT NULL',
            'rated': 'CHAR(10) NOT NULL',
            'runtime': 'TINYINT UNSIGNED NOT NULL',
            'plot': 'TEXT NOT NULL',
            'imdb_rating': 'DECIMAL(4,2) NOT NULL',
            'PRIMARY KEY': '(movie_id)'
        })

        create_table(cursor, 'genre', {
            'genre_id': 'TINYINT AUTO_INCREMENT',
            'name': 'CHAR(20) NOT NULL',
            'PRIMARY KEY': '(genre_id)',
            'CONSTRAINT': 'genre_unique UNIQUE (name)'
        })

        create_table(cursor, 'movie_genre', {
            'movie_id': 'CHAR(10)',
            'genre_id': 'TINYINT',
            'PRIMARY KEY': '(movie_id,genre_id)',
            'FOREIGN KEY (genre_id)': 'REFERENCES genre(genre_id)',
            'FOREIGN KEY (movie_id)': 'REFERENCES movie(movie_id)'
        })

        create_table(cursor, 'director', {
            'director_id': 'SMALLINT AUTO_INCREMENT',
            'name': 'VARCHAR(100) NOT NULL',
            'PRIMARY KEY': '(director_id)',
            'CONSTRAINT': 'director_unique UNIQUE (name)'
        })

        create_table(cursor, 'movie_director', {
            'movie_id': 'CHAR(10)',
            'director_id': 'SMALLINT',
            'PRIMARY KEY': '(movie_id,director_id)',
            'FOREIGN KEY (director_id)': 'REFERENCES director(director_id)',
            'FOREIGN KEY (movie_id)': 'REFERENCES movie(movie_id)'
        })

        create_table(cursor, 'actor', {
            'actor_id': 'SMALLINT AUTO_INCREMENT',
            'name': 'VARCHAR(100) NOT NULL',
            'PRIMARY KEY': '(actor_id)',
            'CONSTRAINT': 'actor_unique UNIQUE (name)'
        })

        create_table(cursor, 'movie_actor', {
            'movie_id': 'CHAR(10)',
            'actor_id': 'SMALLINT',
            'PRIMARY KEY': '(movie_id,actor_id)',
            'FOREIGN KEY (actor_id)': 'REFERENCES actor(actor_id)',
            'FOREIGN KEY (movie_id)': 'REFERENCES movie(movie_id)'
        })


def add_index():
    # after the DB is stable and we added the data, create the indexes.
    con = mysql.connector.connect(**config())
    with con:
        cursor = con.cursor()
        try:
            cursor.execute(f'''
            CREATE FULLTEXT INDEX idx ON movie(plot, title)
            ''')
        except:
            print('Failed to create a fulltext index to movie table plot and title.')
        # note -Index creation is automatically commited after execution.


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Run with 0 to create DB. Run with 1 to create Indexes for the DB.")
        sys.exit(1)
    option = int(sys.argv[1])
    if option == 0:
        create_database()
    if option == 1:
        add_index()
