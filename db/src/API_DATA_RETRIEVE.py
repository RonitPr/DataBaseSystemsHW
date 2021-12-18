# The code which inserts data to our DB.

import csv
from random import expovariate
import mysql.connector
from environment import config, base_url
import urllib.request
import json
import sys


def get_csv_data(filename):
    # into a new file, write 10 lines of 1000 movie ids, seperated by spaces.
    count = 1
    try:
        with open(filename, 'r', encoding='latin-1') as csvFile:
            r = csv.reader(csvFile)
            try:
                open('csv_movie_ids.txt')
            except:
                with open('csv_movie_ids.txt', 'w') as output:
                    for row in r:
                        if count > 10000:
                            break
                        if row[1] == 'movie' and row[5].isdecimal() and int(row[5]) > 1990:
                            if count % 1 == 0 and count != 0:  # TODO - change back to 1000
                                output.write(f'{row[0]}\n')
                            elif count == 10000:
                                output.write(f'{row[0]}')
                            else:
                                output.write(f'{row[0]} ')
                            count += 1
    except:
        return "failed to get CSV data"


def create_list_of(data, data_key):
    l = []
    for i in data[data_key].split(', '):
        l.append(i)
    return l


def insert_movie(data):
    # set up values to go into the inserts.
    runtime = data['Runtime']
    for word in runtime.split():
        if word.isdigit():
            runtime = word
            break
    genre_list = create_list_of(data, 'Genre')
    director_list = create_list_of(data, 'Director')
    actor_list = create_list_of(data, 'Actors')

    # set up the queries for inserting the data to the tables without foreigh keys.
    insert_movie_query = f'''INSERT INTO movie (movie_id, title, year, rated, runtime, plot, box_office, imdb_rating) 
        VALUES ('{data['imdbID']}','{data['Title']}','{data['Year']}','{data['Rated']}','{runtime}','{data['Plot']}','{data['BoxOffice']}','{data['imdbRating']}');
        '''
    insert_genre_queries = []
    for genre in genre_list:
        insert_genre_queries.append(f'''INSERT IGNORE INTO genre (name)
            VALUES ('{genre}');
            ''')

    insert_director_queries = []
    for director in director_list:
        insert_director_queries.append(f'''INSERT IGNORE INTO director (name)
            VALUES ('{director}');
            ''')

    insert_actor_queries = []
    for actor in actor_list:
        insert_actor_queries.append(
            f'''INSERT IGNORE INTO actor (name) VALUES ('{actor}');
            ''')
    # TODO - execute and commit.

    # set up the queries for inserting the data to the tables with foreigh keys.
    insert_moviegenre_queries = []
    for genre in genre_list:
        insert_moviegenre_queries.append(f'''INSERT INTO movie_genre (movie_id, genre_id)
            VALUES ('{data['imdbID']}','{1}');
            ''')

    insert_moviedirector_queries = []
    for director in director_list:
        insert_moviedirector_queries.append(f'''INSERT INTO movie_director (movie_id, director_id)
            VALUES ('{data['imdbID']}','{1}');
            ''')

    insert_movieactor_queries = []
    for actor in actor_list:
        insert_movieactor_queries.append(
            f'''INSERT INTO movie_actor (movie_id, actor_id) VALUES ('{data['imdbID']}','{1}');
            ''')

    # connect to db and execute the queries.
    print(" ".join(insert_actor_queries))
    # con = mysql.connector.connect(**config())
    # with con:
    #     cursor = con.cursor()
    #     try:
    #         cursor.execute(insert_movie_query)
    #         # more here
    #         con.commit()
    #     except:
    #         print("Failed executing the insertion queries.")
    #         con.rollback()


def insert_movies_batch(line):
    # get one line from the csv file, fetch and insert to the tables all the movies from it.
    ids_list = line.split(" ")
    for id in ids_list:
        params = urllib.parse.urlencode({'i': id})
        with urllib.request.urlopen(base_url() + params) as response:
            res = response.read()
            res = json.loads(res)
        insert_movie(res)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please provide the line number you want to insert to the db.")
        sys.exit(1)
    line_index = int(sys.argv[1])

    get_csv_data("imdb_data.csv")
    try:
        with open('csv_movie_ids.txt', 'r') as in_file:
            count = 0
            for line in in_file:
                count += 1
                if count == line_index:
                    stripped_line = line.strip()
                    insert_movies_batch(stripped_line)

    except:
        print('failed to open the csv_movie_ids.txt file.')
