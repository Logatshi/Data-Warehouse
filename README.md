# Data-Warehouse
# Sparkify Data Warehouse

## Purpose of the Database

The Sparkify database is designed to support the analytical objectives of Sparkify, a music streaming startup. The primary goal of the database is to enable data-driven insights into user behavior, song popularity, and engagement patterns, thereby enhancing the overall user experience.

### Analytical Goals

1. **User Behavior Analysis**: To understand how users interact with the platform, including which songs and genres are most popular and the times of day users are most active.
2. **Content Optimization**: To identify frequently played songs, informing decisions around content acquisition and marketing strategies.
3. **User Retention**: To analyze factors influencing user retention and churn rates, providing insights for improving user engagement.
4. **Trend Analysis**: To track changes in music consumption patterns over time, enabling Sparkify to adapt its offerings to user preferences.

## Database Schema Design

### Justification of Schema Design

The database employs a star schema design to optimize query performance and enhance reporting capabilities. This design structure separates the fact and dimension tables, allowing for efficient data retrieval and analysis.

### Schema Overview

- **Fact Table: `songplays`**
  - **Columns**:
    - `songplay_id`: Unique identifier for each song play (Primary Key)
    - `start_time`: Timestamp indicating when the song was played
    - `user_id`: Unique identifier for each user
    - `level`: User subscription level (e.g., free, paid)
    - `song_id`: Unique identifier for the song
    - `artist_id`: Unique identifier for the artist
    - `session_id`: Unique identifier for each user session
    - `location`: Geographical location of the user
    - `user_agent`: User agent string from the web client

- **Dimension Tables**:
  - **`users`**
    - `user_id`: Unique identifier for each user (Primary Key)
    - `first_name`: User's first name
    - `last_name`: User's last name
    - `gender`: User's gender
    - `level`: User subscription level (e.g., free, paid)

  - **`songs`**
    - `song_id`: Unique identifier for each song (Primary Key)
    - `title`: Title of the song
    - `artist_id`: Unique identifier for the artist
    - `year`: Year the song was released
    - `duration`: Duration of the song in seconds

  - **`artists`**
    - `artist_id`: Unique identifier for each artist (Primary Key)
    - `name`: Name of the artist
    - `location`: Location of the artist
    - `latitude`: Latitude of the artist's location
    - `longitude`: Longitude of the artist's location

  - **`time`**
    - `start_time`: Timestamp (Primary Key)
    - `hour`: Hour of the day
    - `day`: Day of the month
    - `week`: Week of the year
    - `month`: Month of the year
    - `year`: Year
    - `weekday`: Day of the week

## ETL Pipeline

The ETL pipeline for the Sparkify database consists of the following steps:

1. **Extract**:
   - Data is extracted from source files in JSON format, which contain user activity, song metadata, and artist details.

2. **Transform**:
   - The extracted data is cleaned and formatted to ensure consistency and remove duplicates.
   - Derived tables are created, such as `songplays`, by mapping relevant fields from the source data to the corresponding columns in the fact and dimension tables.

3. **Load**:
   - The transformed data is loaded into the star schema of the Sparkify database, populating both fact and dimension tables.

This schema design and ETL pipeline provide Sparkify with a robust foundation for data analysis, enabling the company to derive valuable insights and make informed decisions.

![Data Warehouse](https://github.com/user-attachments/assets/cbfea8c3-55a3-408d-bfe9-6b6ae3e503d9)
