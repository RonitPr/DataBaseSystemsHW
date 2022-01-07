def query_1(cursor, values):
    sql = '''SELECT g.name AS genre, AVG(m.imdb_rating) AS average_rating
    FROM movie m, movie_genre mg, genre g
    WHERE m.year >= DATE_SUB(CURDATE(), INTERVAL %s YEAR)
	    AND m.movie_id = mg.movie_id
	    AND mg.genre_id = g.genre_id 
    GROUP BY g.name
    ORDER BY average_rating DESC
    LIMIT 1
    '''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_2(cursor):
    sql = '''SELECT g.name, COUNT(*)
    FROM genre AS g
    JOIN movie_genre AS mg ON g.genre_id = mg.genre_id
    JOIN movie AS m ON m.movie_id = mg.movie_id
    GROUP BY g.genre_id
    ORDER BY COUNT(*)
    LIMIT 1
    '''
    try:
        cursor.execute(sql)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_3(cursor, values):
    sql = '''SELECT DISTINCT d.name as director
    FROM director AS d
    JOIN movie_director AS md ON d.director_id = md.director_id
    WHERE md.movie_id IN
	    (
	    SELECT movie_id
	    FROM movie
	    WHERE MATCH (title, plot)
	    AGAINST (%s)
    	)
    '''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_4(cursor, values):
    sql = ''''''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_5(cursor, values):
    sql = ''''''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_6(cursor, values):
    sql = '''SELECT a.name, AVG(m.imdb_rating) AS avg_rating
    FROM actor AS a
    JOIN movie_actor AS ma ON a.actor_id = ma.actor_id
    JOIN movie AS m ON ma.movie_id = m.movie_id
    WHERE m.rated = %s
    GROUP BY a.actor_id
    ORDER BY AVG(m.imdb_rating) DESC
    LIMIT 3
    '''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")


def query_7(cursor, values):
    sql = '''
    SELECT AVG(runtime) as recommended_runtime
    FROM
	    (
        SELECT runtime
	    FROM movie AS m
	    JOIN movie_genre AS mg ON m.movie_id = mg.movie_id
	    JOIN genre AS g ON mg.genre_id = g.genre_id
	    WHERE m.rated = %s
	    AND g.name = %s
	    ORDER BY m.imdb_rating DESC
	    LIMIT 10
        ) AS top_10
'''
    try:
        cursor.execute(sql, values)
    except Exception as e:
        print(f"Error executing query with error: {e}")
