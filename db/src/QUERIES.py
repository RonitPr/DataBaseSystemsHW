def query_1(cursor, year):
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
        cursor.execute(sql, year)
    except Exception as e:
        print(f"Error executing query with error: {e}")
    res = cursor.fetchall()
    return res


def query_2(cursor, year):  # TODO
    sql = '''SELECT g.name, COUNT(*)
    FROM genre AS g
    JOIN movie_genre AS mg ON g.genre_id = mg.genre_id
    JOIN movie AS m ON m.movie_id = mg.movie_id
    WHERE m.year >= DATE_SUB(CURDATE(), INTERVAL %s YEAR)
    GROUP BY g.genre_id
    ORDER BY COUNT(*) 
    LIMIT 1
    '''
    try:
        cursor.execute(sql, year)
    except Exception as e:
        print(f"Error executing query with error: {e}")
    res = cursor.fetchall()
    return res


def query_3(cursor, keyWord):
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
        cursor.execute(sql, keyWord)
    except Exception as e:
        print(f"Error executing query with error: {e}")
    res = cursor.fetchall()
    return res


def query_4(cursor, actorName):
    sql = '''SELECT a.name AS actor_name, AVG(m.imdb_rating) AS avg_imdb_rating
    FROM actor AS a
    JOIN movie_actor AS ma ON a.actor_id = ma.actor_id
    JOIN movie AS m ON ma.movie_id = m.movie_id
    WHERE a.actor_id IN
        (
	    SELECT DISTINCT ma2.actor_id AS co_actors
	    FROM actor AS a
	    JOIN movie_actor AS ma1 ON a.actor_id = ma1.actor_id
	    JOIN movie_actor AS ma2 ON ma1.movie_id = ma2.movie_id
	    WHERE a.name = %s
	    AND ma1.actor_id <> ma2.actor_id
        )
    GROUP BY a.name
    ORDER BY AVG(m.imdb_rating) DESC
    LIMIT 2
    '''
    try:
        cursor.execute(sql, actorName)
    except Exception as e:
        print(f"Error executing query with error: {e}")
    res = cursor.fetchall()
    return res


def query_5(cursor, directorName):
    sql = '''SELECT a.name AS actor
    FROM actor AS a
    JOIN movie_actor AS ma ON a.actor_id = ma.actor_id
    JOIN movie_director AS md ON ma.movie_id = md.movie_id
    JOIN director AS d ON md.director_id = d.director_id
    WHERE d.name = %s
    GROUP BY a.name, d.name
    ORDER BY COUNT(*) DESC
    LIMIT 3
    '''
    try:
        cursor.execute(sql, directorName)
    except Exception as e:
        print(f"Error executing query with error: {e}")
    res = cursor.fetchall()
    return res


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
    res = cursor.fetchall()
    return res


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
    res = cursor.fetchall()
    return res
