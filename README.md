Mirrulation Data Hacking
===============================

Tools for working towards de-duplicating regulation comments.

* the schema to the database is db.sql
* the json extract tool is import_json.py
  * To run it, put your database credentials into a .env file and put the .env file in the main directory
  * And then copy the .env file to the /trottertools/mySQLh/ directory
* the tool to hit the ChatGPT etc api is ask_ChatGPT.py
* there is a copy of the recent DEA extract available under /data/ if you want to look at the results

Per File Docs
-----------------

* import_regulation_json_into_DB.py - imports the mirrulations json comment data into a MYSQL database
* ask_ChatGPT.py  - loops over the comments in the database and asks questions to chatGPT regarding each comment
* duplicate_comment_detection.py - compares comments to other comments.
