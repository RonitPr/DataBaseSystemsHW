import csv
import mysql.connector
from environment import config, base_url
import urllib.request
import json
import sys

# utility functions


def get_list_of(data, data_key):
    l = []
    for i in data[data_key].split(', '):
        l.append(i)
    return l


def check_nulls(values):
    for value in values:
        if value == 'N/A' or value == '/N':
            return True
    return False


def get_int_boxoffice(str):
    s = ""
    for word in str[1:].split(","):
        s += word
    return int(s)


def get_runtime(str):
    for word in str.split():
        if word.isdigit():
            return word

# get data:


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
                        if count > 30000:
                            break
                        if row[1] == 'movie' and row[5].isdecimal() and int(row[5]) > 2000:
                            if count % 1000 == 0 and count != 0:
                                output.write(f'{row[0]}\n')
                            elif count == 30000:
                                output.write(f'{row[0]}')
                            else:
                                output.write(f'{row[0]} ')
                            count += 1
    except:
        return "failed to get CSV data"


def get_main_tables_insert_queries(data, actor_list, director_list, genre_list):
    # return insert queries for movie, actor, director, genre in format:
    # [('query with %s',(values))]
    q = []
    q.append(('INSERT INTO movie (movie_id, title, year, rated, runtime, plot, box_office, imdb_rating) VALUES(%s,%s,%s,%s,%s,%s,%s,%s);',
              (data['imdbID'], data['Title'],
               data['Year'], data['Rated'],
               get_runtime(data['Runtime']),
               data['Plot'],
               get_int_boxoffice(data['BoxOffice']),
               data['imdbRating'])))
    for genre in genre_list:
        q.append(('INSERT IGNORE INTO genre (name) VALUES (%s);', [genre]))
    for director in director_list:
        q.append(
            ('INSERT IGNORE INTO director (name) VALUES (%s);', [director]))
    for actor in actor_list:
        q.append(('INSERT IGNORE INTO actor (name) VALUES (%s);', [actor]))
    return q


def get_secondary_tables_insert_queries(con, data, actor_list, director_list, genre_list):
    # return insert queries for movie_actor, movie_director, movie_genre in format:
    # [('query with %s',(values))]
    q = []
    cursor2 = con.cursor(buffered=True)
    for genre in genre_list:
        cursor2.execute(
            f'''SELECT genre_id from genre where name = '{genre}';''')
        id = cursor2.fetchone()[0]
        q.append(('INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s,%s)',
                  (data['imdbID'], id)))
    for director in director_list:
        cursor2.execute(
            f'''SELECT director_id from director where name = '{director}';''')
        id = cursor2.fetchone()[0]
        q.append(('INSERT INTO movie_director (movie_id, director_id) VALUES (%s,%s)',
                  (data['imdbID'], id)))
    for actor in actor_list:
        cursor2.execute(
            f'''SELECT actor_id from actor where name = '{actor}';''')
        id = cursor2.fetchone()[0]
        q.append(('INSERT INTO movie_actor (movie_id, actor_id) VALUES (%s,%s)',
                  (data['imdbID'], id)))
    return q


def insert_single_movie(con, data):
    # with data that was checked for harmful nulls, execute inserts for mall tables
    al = get_list_of(data, 'Actors')
    dl = get_list_of(data, 'Director')
    gl = get_list_of(data, 'Genre')
    cursor = con.cursor(prepared=True)
    q = get_main_tables_insert_queries(data, al, dl, gl)
    for query, values in q:
        try:
            cursor.execute(query, values)
        except Exception as e:
            con.rollback()
            print(f'Failed to {query} with values: {values}. Rolling back.')
            raise e
    con.commit()

    q = get_secondary_tables_insert_queries(con, data, al, dl, gl)
    for query, values in q:
        try:
            cursor.execute(query, values)
        except Exception as e:
            con.rollback()
            print(
                f'Failed to {query} with values: {values}. Rolling back for secondary tables!!')
            raise e
    con.commit()


def insert_movies_batch(line):
    con = mysql.connector.connect(**config())
    count_insertions = 0
    ids_list = line.split(" ")
    for id in ids_list:
        params = urllib.parse.urlencode({'i': id})
        with urllib.request.urlopen(base_url() + params) as response:
            res = response.read()
            res = json.loads(res)
        if not check_nulls([res['Year'], res['Runtime'], res['BoxOffice'], res['imdbRating']]):
            try:
                insert_single_movie(con, res)
                count_insertions += 1
            except Exception as e:
                print(
                    f'Failed inserting movie {id} with exception: {e}. Skipping..')
    print('**********')
    print(f'Successfuly inserted {count_insertions} movies into DB.')
    print('**********')


def clear_all_tables():
    con = mysql.connector.connect(**config())
    cursor = con.cursor(prepared=True)
    clear_q = ['DELETE FROM movie_actor;', 'DELETE FROM movie_director;', 'DELETE FROM movie_genre;',
               'DELETE FROM movie;', 'DELETE FROM actor;', 'DELETE FROM director;', 'DELETE FROM genre;']
    reset_ids_q = ['ALTER TABLE actor AUTO_INCREMENT = 1;',
                   'ALTER TABLE director AUTO_INCREMENT = 1;',
                   'ALTER TABLE genre AUTO_INCREMENT = 1;']
    for q in clear_q + reset_ids_q:
        cursor.execute(q)
    con.commit()


def alterLMAO():
    con = mysql.connector.connect(**config())
    cursor = con.cursor()
    cursor.execute('''ALTER TABLE movie MODIFY rated CHAR( 10 ) NOT NULL''')


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please provide the line number you want to insert to the db.")
        sys.exit(1)
    line_index = int(sys.argv[1])
    get_csv_data("imdb_data.csv")
    if line_index == 404:
        clear_all_tables()
    try:
        with open('csv_movie_ids.txt', 'r') as in_file:
            count = 0
            for line in in_file:
                count += 1
                if count == line_index:
                    stripped_line = line.strip()
                    insert_movies_batch(stripped_line)
    except Exception as e:
        print(
            f'Failed to open the csv_movie_ids.txt file and insert the data: {e}')
