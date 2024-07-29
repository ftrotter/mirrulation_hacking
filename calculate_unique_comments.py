from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh
import os
import json
import sys
import sqlalchemy as db
import re
import difflib
from difflib import unified_diff
from sqlalchemy import select
from sqlalchemy import text
import MySQLdb
from difflib import HtmlDiff
import argparse


if __name__ == "__main__":

    
    comment_table = 'comment'
    m_db = 'mirrulation'
    commentDBT = myDBTable.myDBTable(m_db, comment_table)

    unique_comment_DBT = myDBTable.myDBTable(m_db,'uniquecomment')
    cluster_DBT = myDBTable.myDBTable(m_db,'comment_cluster')
    unique_to_comment_DBT = myDBTable.myDBTable(m_db,'uniquecomment_to_comment')

#    if len(sys.argv) < 2:
#        print("Usage: python import_json.py <directory_path>")
#        sys.exit(1)


    sql_dict = WriteOnceDict.WriteOnceDict()

    sql_dict['update the md5 on the comment field'] = f""" 
UPDATE  mirrulation.comment
SET `simplified_comment_text_md5` = MD5(`simplified_comment_text`)
"""

    sql_dict['drop unique table'] = f"DROP TABLE IF EXISTS {unique_comment_DBT}"

    spaces = '                                                     '
    # Lots of spaces because we want to support very long wayback machine urls.
    sql_dict['create unique table']  =  f"""
CREATE TABLE {unique_comment_DBT}
SELECT
    simplified_comment_text_md5,
    simplified_comment_text,
    COUNT(DISTINCT(commentId)) AS comment_count
FROM {commentDBT}
GROUP BY simplified_comment_text_md5
ORDER BY comment_count DESC
""" 

    sql_dict['add an id as the primary key'] = f"ALTER TABLE {unique_comment_DBT}  ADD `id` INT(11) NOT NULL AUTO_INCREMENT  FIRST,  ADD   PRIMARY KEY  (`id`);"    

    sql_dict['index the md5'] = f"ALTER TABLE {unique_comment_DBT} ADD UNIQUE(`simplified_comment_text_md5`); "

    sql_dict['drop the unique to comment map'] = f"DROP TABLE IF EXISTS {unique_to_comment_DBT}"

    sql_dict['create the unique to comment map'] = f"""
CREATE TABLE {unique_to_comment_DBT} AS 
SELECT 
    acomment.id AS comment_id,
    uniquecomment.id AS uniquecomment_id

FROM  {unique_comment_DBT} AS uniquecomment 
JOIN {commentDBT} AS acomment ON 
    acomment.simplified_comment_text_md5 = 
    uniquecomment.simplified_comment_text_md5
ORDER BY acomment.id, uniquecomment.id
"""

    SQLh = mySQLh.mySQLh()

    is_just_print = False
    SQLh.runQuerys(sql_dict,is_just_print)
