# A demo of the local libraries

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


hdiff = HtmlDiff()


def compare_strings(str1, str2):
    """
    Compares two strings using difflib and returns a numerical score of their similarity.

    Args:
        str1 (str): The first string to compare.
        str2 (str): The second string to compare.

    Returns:
        float: A numerical score between 0 and 1 representing the similarity between the two strings.
    """
    sequence_matcher = difflib.SequenceMatcher(None, str1, str2)
    return sequence_matcher.ratio()



if __name__ == "__main__":

    m_db = 'mirrulation'
    comment_table = 'comment'
    commentDBT = myDBTable.myDBTable(m_db,comment_table)

    unique_comment_DBT = myDBTable.myDBTable(m_db,'unique_comment')


#    if len(sys.argv) < 2:
#        print("Usage: python import_json.py <directory_path>")
#        sys.exit(1)


    sql_dict = WriteOnceDict.WriteOnceDict()

    sql_dict['drop unique table'] = f"DROP TABLE IF EXISTS {unique_comment_DBT}"

    sql_dict['create unique table']  =  f"""
CREATE TABLE {unique_comment_DBT}
SELECT 
    simplified_comment_text,
    COUNT(DISTINCT(commentId)) AS comment_count
FROM {commentDBT}
GROUP BY simplified_comment_text
ORDER BY comment_count DESC
""" 

    sql_dict['add an id as the prrimary key'] = f"ALTER TABLE {unique_comment_DBT}  ADD `id` INT(11) NOT NULL AUTO_INCREMENT  FIRST,  ADD   PRIMARY KEY  (`id`);"    
    
    #sql_dict['clear the comment cluster table'] = f"TRUNCATE TABLE {unique_comment_DBT}"


    SQLh = mySQLh.mySQLh()

    is_just_print = False
    SQLh.runQuerys(sql_dict,is_just_print)

    #empty the sql_dictionary
    sql_dict = WriteOnceDict.WriteOnceDict()

    all_unique_comments = []

    is_first_run = True
    result = SQLh._conn.execute(f"SELECT * FROM {unique_comment_DBT}")
    for unique_comment_row in result:
        all_unique_comments.append(unique_comment_row)
        
    # At what score should we say "these are basically the same?"
    threshold = .85


    outer_i = 0        
    for outside_row in all_unique_comments:
        outer_i = outer_i + 1
        print(f"\t\t\t\t\tLooping over outer: {outer_i}")
        inner_j = 0
        for inside_row in all_unique_comments: 
            inner_j = inner_j + 1
            #print(f"Looping over inner: {inner_j}")
            score = compare_strings(outside_row.simplified_comment_text,inside_row.simplified_comment_text)
            score = round(score,4)
            if(score > threshold):
                o_id = outside_row.id
                i_id = inside_row.id
                matching_string = f"Match between {o_id} and {i_id}"
                print(matching_string)
                print(outside_row.simplified_comment_text[:70])
                print(inside_row.simplified_comment_text[:70])
                print(f"Score: {score}")
                if (o_id != i_id):
                    difflist = unified_diff(outside_row.simplified_comment_text.splitlines(),inside_row.simplified_comment_text.splitlines())
                    diff_text = "\n".join(list(difflist)) 
                    e_diff_text = MySQLdb.escape_string(diff_text).decode('utf-8')
                    sql_dict[matching_string] = f"""
INSERT INTO `comment_clusters` 
(`unique_comment_id`, `other_unique_comment_id`, `score`, `diff_text`) 
VALUES ('{o_id}', '{i_id}', '{score}', 

'{e_diff_text}' 

);

"""
                else:
                    print("Everything always matches itself")

        # This will run on every outside loop
        SQLh.runQuerys(sql_dict,is_just_print)
        sql_dict = WriteOnceDict.WriteOnceDict()

