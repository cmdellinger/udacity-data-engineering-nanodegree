# Project: Data Modeling with Postgres

The purpose of this project is to create an ETL pipline that transfers data from two types of JSON log files stored in directory trees into a Postres database using Python and SQL.

The source data is song and user activity logs from a simulated music streaming app, Sparkify. Once transformed the data will be inserted into the Postgres database created using a star schema. This less normalized database design was chosen due to the fictional firm's desire to optimize queries for song play analysis.

---

## Data Files

### _Song Dataset_

The song data is stored in directory trees based on the first three letters of each song's track ID.

_Example file structure_:
```javascript
song_data/A/A/C/TRAACCG128F92E8A55.json
```

### _Log Dataset_

The log data contains simlated event data for the music streaming app.

_Example file structure_:
```javascript
log_data/2018/11/2018-11-01-events.json
log_data/2018/11/2018-11-02-events.json
```

---

## Database Schema
The table schema of the Postgres database will be created using a **_star schema_**. Although the schema is slightly denormalized with some duplicate data fields between tables, it will optimize the song play queries that Sparkify would like to run.

<img id="Sparkify_SQL_Schema" src="/images/Sparkify_SQL_Schema.png" onerror="this.onerror=null; this.src="https://github.com/cmdellinger/udacity-data-engineering-nanodegree/blob/master/02%20-%20Data%20Modeling/03%20-%20project%20-%20data%20modeling%20with%20postgres/project/images/Sparkify_SQL_Schema.png" alt="">

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
- PostreSQL
- Python 3.6+
    + pandas
    + psycopg2

<b>Usage</b>:
- Run `python create_tables.py` to create the tables in the database.
    + `etl.ipynb` and `test.ipynb` can now be run, but the tables are empty.
- Run `etl.py` to load the data into the database tables.
    + Queries in `test.ipynb` should now return results.

<b>Note</b>:<br>
`create_tables.py` needs to be run after any edit in `sql_queries.py`.
