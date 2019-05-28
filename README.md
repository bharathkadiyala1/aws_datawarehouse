# Data Warehouse on AWS

__1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.__

Sparkify's rapid user growth calls for better analytical organisation and infrastructure. The purpose of this database is to organize user data into an industry standard __Star Schema__ with one fact table and some dimension tables on the AWS cloud platform.

__2. State and justify your database schema design and ETL pipeline.__

Data Engineers at Sparkify believe that the __Star Schema__ is the best way to organize user data.

__`songplays`__ is our fact table with the following columns with `songplay_id` & `user_id` as primary key:
- songplay_id (Auto generated)
- start_time (Cannot be NULL)
- user_id (Cannot be NULL)
- level
- song_id
- artist_id
- session_id
- location
- user_agent


__`users`__ is one of the dimension table with the following columns:
- user_id (Cannot be NULL)
- first_name 
- last_name 
- gender
- level (Cannot be NULL)


__`songs`__ is one of the dimension table with the following columns:
- song_id (Cannot be NULL)
- title (Cannot be NULL)
- artist_id (Cannot be NULL)
- year
- duration


__`artists`__ is one of the dimension table with the following columns:
- artist_id (Cannot be NULL)
- name (Cannot be NULL)
- location
- latitude
- longitude


__`time`__ is one of the dimension table with the following columns:
- start_time (Cannot be NULL)
- hour (Cannot be NULL)
- day (Cannot be NULL)
- week (Cannot be NULL)
- month (Cannot be NULL)
- year (Cannot be NULL)
- weekday (Cannot be NULL)

