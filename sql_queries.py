import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

ARN = config.get("IAM_ROLE","ARN")
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stage"
staging_songs_table_drop  = "DROP TABLE IF EXISTS songs_stage"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= "CREATE TABLE IF NOT EXISTS events_stage   				\
		                        (                                       				\
		                            artist 			VARCHAR,							\
		                            auth 			VARCHAR,              				\
		                            firstName 		VARCHAR,							\
		                            gender			VARCHAR,               				\
		                            itemInSession	INTEGER,                			\
		                            lastName 		VARCHAR,                			\
		                            length  		REAL,                				\
		                            level    		VARCHAR,                			\
		                            location  		VARCHAR,                			\
		                            method  		VARCHAR,                			\
		                            page  			VARCHAR,                			\
		                            registration  	REAL,	                			\
		                            sessionId  		VARCHAR,                			\
		                            song  			VARCHAR,                			\
		                            status  		VARCHAR,                			\
		                            ts  			BIGINT,                				\
		                            userAgent  		VARCHAR,                			\
		                            userId  		VARCHAR                 			\
		                        );"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS songs_stage   					\
		                        (                                       				\
		                            num_songs 			INTEGER,						\
		                            artist_id 			VARCHAR,              			\
		                            artist_latitude 	REAL,							\
		                            artist_longitude	REAL,               			\
		                            artist_location		VARCHAR,                		\
		                            artist_name 		VARCHAR,                		\
		                            song_id  			VARCHAR,                		\
		                            title    			VARCHAR,                		\
		                            duration  			REAL,                			\
		                            year  				INTEGER                			\
		                        );"

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays   				\
                        (														\
                        	songplay_id BIGINT identity(0, 1),                  \
                            start_time  TIMESTAMP NOT NULL,             		\
                            user_id     VARCHAR NOT NULL,						\
                            level       VARCHAR,                				\
                            song_id     VARCHAR,                				\
                            artist_id   VARCHAR,                				\
                            session_id  VARCHAR,                				\
                            location    VARCHAR,                				\
                            user_agent  VARCHAR,                				\
                            PRIMARY KEY (songplay_id, user_id)  				\
                        );"

user_table_create = "CREATE TABLE IF NOT EXISTS users 					   		\
						(					                               		\
							user_id 	VARCHAR NOT NULL,        				\
							first_name 	VARCHAR,              					\
							last_name 	VARCHAR,               					\
							gender 		VARCHAR,                  				\
							level 		VARCHAR NOT NULL,		 				\
							PRIMARY KEY (user_id)                          		\
						) DISTSTYLE ALL;								   		\
					"

song_table_create = "CREATE TABLE IF NOT EXISTS songs 					   		\
						(					                               		\
							song_id 	VARCHAR NOT NULL,        				\
							title 		VARCHAR,                 				\
							artist_id 	VARCHAR distkey,        				\
							year 		INTEGER,								\
							duration 	REAL,				         		    \
							PRIMARY KEY (song_id)                          		\
						);												   		\
					"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists       				\
                        (                                       				\
                            artist_id 	VARCHAR NOT NULL,       				\
                            name       	VARCHAR,			      				\
                            location   	VARCHAR,			  					\
                            latitude    REAL,                   				\
                            longitude   REAL,                   				\
                            PRIMARY KEY (artist_id)             				\
                        ) DISTSTYLE AUTO;"

time_table_create = "CREATE TABLE IF NOT EXISTS time            				\
                        (                                       				\
                            start_time  TIMESTAMP 	NOT NULL,     				\
                            hour        INTEGER 	NOT NULL,           		\
                            day         INTEGER	 	NOT NULL,           		\
                            week        INTEGER 	NOT NULL,           		\
                            month   	INTEGER 	NOT NULL,           		\
                            year   		INTEGER 	NOT NULL,           		\
                            weekday   	INTEGER 	NOT NULL,           		\
                            PRIMARY KEY (start_time)            				\
                        ) DISTSTYLE AUTO;"

# STAGING TABLES

staging_events_copy = ("""COPY events_stage FROM '{}' CREDENTIALS 'aws_iam_role={}' FORMAT AS JSON '{}'""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy  = ("""COPY songs_stage FROM '{}' CREDENTIALS 'aws_iam_role={}' FORMAT AS JSON 'auto'""").format(SONG_DATA, ARN, LOG_JSONPATH)

# FINAL TABLES

songplay_table_insert = "	INSERT INTO songplays(	start_time, 												\
													user_id, 													\
													level, 														\
													song_id, 													\
													artist_id, 													\
													session_id, 												\
													location, 													\
													user_agent													\
												 )																\
							SELECT 	TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 sec' AS start_time,		\
									events.userId AS user_id,													\
        							events.level AS level,														\
        							songs.song_id AS song_id,													\
        							songs.artist_id AS artist_id,												\
        							events.sessionId AS session_id,												\
        							events.location AS location,												\
        							events.userAgent AS user_agent												\
							FROM 	events_stage events 														\
									JOIN songs_stage songs 														\
										ON 	events.artist=songs.artist_name 									\
										AND events.song=songs.title;											\
						"

user_table_insert = "	INSERT INTO users(user_id, first_name, last_name, gender, level)	\
						SELECT userId, firstName, lastName, gender, level 					\
						FROM events_stage													\
						WHERE CONCAT(userId, ts) in (	SELECT CONCAT(userId, MAX(ts))		\
                             							FROM events_stage 					\
                             							GROUP BY userId						\
                             						);										\
                    "

song_table_insert = "	INSERT INTO songs (song_id, title, artist_id, year, duration)		\
						SELECT song_id, title, artist_id, year, duration					\
						FROM songs_stage;													\
					"

artist_table_insert = 	"	INSERT INTO artists (artist_id, name, location, latitude, longitude)							\
							SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude		\
							FROM songs_stage																				\
							WHERE CONCAT(artist_id, year) IN (	SELECT CONCAT(artist_id, MAX(year)) 						\
																FROM songs_stage 											\
																GROUP BY artist_id											\
															 );																\
						"

time_table_insert = "	INSERT INTO time (start_time, hour, day, week, month, year, weekday)	\
						SELECT a.ts 				AS start_time, 								\
						CAST (to_char(a.ts, 'HH') 	AS INTEGER) AS hour, 						\
        				CAST (to_char(a.ts, 'DD') 	AS INTEGER) AS day, 						\
        				CAST (to_char(a.ts, 'W')  	AS INTEGER) AS week, 						\
        				CAST (to_char(a.ts, 'MM') 	AS INTEGER) AS month, 						\
        				CAST (to_char(a.ts, 'YYYY') AS INTEGER) AS year,						\
        				CAST (to_char(a.ts, 'D') 	AS INTEGER) AS weekday						\
						FROM 	(	SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 sec' AS ts \
									FROM events_stage 											\
      							) a;															\
      				"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, time_table_insert, song_table_insert, artist_table_insert, songplay_table_insert]
