# Project: Data Modeling with Postgres

The purpose of this project is to create an ETL pipline that transfers data from two types of JSON log files stored in directory trees into a Postres database using Python and SQL.

The source data is song and user activity logs from a simulated music streaming app, Sparkify. Once transformed the data will be inserted into the Postgres database created using a **_Star Schema_**. This less normalized database design was chosen due to the fictional firm's desire to optimize queries on song play analysis.

---

## Log Files

### _Song Dataset_

The song data is stored in directory trees based on the first three letters of each song's track ID. 

_Example file structure_:
```javascript
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
_Example song JSON_:
```javascript
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### _Log Dataset_

The log data contains simlated event data for the music streaming app.

_Example file structure_:
```javascript
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
_Example **row** of log JSON_:
```javascript
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

---

## Database Schema
Once transformed the data will be inserted into the Postgres database created using a **_Star Schema_**. This less normalized database design was chosen due to the fictional firm's desire to optimize queries on song play analysis.

### _Fact Table_
<b>songplays</b> - records in log data associated with song plays i.e. records with page NextSong
+ _contains_: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### _Dimension Tables_
<b>users</b> - users in the app
+ _contains_: user_id, first_name, last_name, gender, level

<b>songs</b> - songs in music database
+ _contains_: song_id, title, artist_id, year, duration

<b>artists</b> - artists in music database
+ _contains_: artist_id, name, location, lattitude, longitude

<b>time</b> - timestamps of records in songplays broken down into specific units
+ _contains_: start_time, hour, day, week, month, year, weekday

---

## Files
<table>
  <tr>
    <th>File</th>
    <th>Description</th> 
  </tr>
  <tr>
    <td>data/</td>
    <td>folder containing song and log data directory trees</td> 
  </tr>
  <tr>
    <td>etl.ipynb</td>
    <td>Jupyter(IPython) notebook with example ETL operations used to develop the ETL functions</td> 
  </tr>
  <tr>
    <td>test.ipynb</td>
    <td>Jupyter(IPython) notebook with test queries to check if data loaded into tables correctly</td> 
  </tr>
  <tr>
    <td>create_tables.py</td>
    <td>Python script that deletes and recreates the database as outlined in sql_queries.py</td> 
  </tr>
  <tr>
    <td>etl.py</td>
    <td>Python script that performs all the ETL operations</td> 
  </tr>
  <tr>
    <td>sql_queries.py</td>
    <td>contains SQL queries as strings for table destruction, creation, & data insertion</td> 
  </tr>
</table>

---

## Usage
<b>Requirements</b>:
* PostreSQL
* Python 3.6+

<b>Usage</b>:
- Run `python create_tables.py` to create the tables in the database.
    + `etl.ipynb` and `test.ipynb` can now be run, but the tables are empty.
- Run `etl.py` to load the data into the database tables.
    + Queries in `test.ipynb` should now return results.

<b>Note</b>:<br>
`create_tables.py` needs to be run after any edit in `sql_queries.py`.

