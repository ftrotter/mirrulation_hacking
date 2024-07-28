# This loops over ever comment and compares it to every other comment using difflib

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
    parser = argparse.ArgumentParser(description='Process comments.')
    parser.add_argument('--comment_source_table', type=str, default='comment', help='Name of the comment table')
    parser.add_argument('--db', type=str, default='mirrulation', help='Name of the comment table')   
    parser.add_argument('--output_table', type=str, default='mirrulation', help='Name of the comment table') 
    args = parser.parse_args()
    
    comment_table = args.comment_source_table
    m_db = args.db
    commentDBT = myDBTable.myDBTable(m_db, comment_table)

    unique_comment_DBT = myDBTable.myDBTable(m_db,'uniquecomment')
    cluster_DBT = myDBTable.myDBTable(m_db,'comment_clusters')
    unique_to_comment_DBT = myDBTable.myDBTable(m_db,'uniquecomment_to_comment')


    sql_dict = WriteOnceDict.WriteOnceDict()

    all_unique_comments = []

    is_first_run = True
    result = SQLh._conn.execute(f"SELECT * FROM {unique_comment_DBT}")
    for unique_comment_row in result:
        all_unique_comments.append(unique_comment_row)
        
    # At what score should we say "these are basically the same?"
    threshold = .85

    list_of_done = []

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
                if i_id in list_of_done:
                    #This is already done.. in the outer loop
                    pass
                else:
                    matching_string = f"Match between {o_id} and {i_id}"
                    print(matching_string)
                    print(outside_row.simplified_comment_text[:70])
                    print(inside_row.simplified_comment_text[:70])
                    print(f"Score: {score}")
                    if (o_id != i_id):
                        #difflist = unified_diff(outside_row.simplified_comment_text.splitlines(),inside_row.simplified_comment_text.splitlines())
                        #diff_text = "\n".join(list(difflist)) 
                        #e_diff_text = MySQLdb.escape_string(diff_text).decode('utf-8')

                        #lets not calculate the diff text for now...
                        #Anything to speed this up..
                    
                        sql_dict[matching_string] = f"""
INSERT INTO {cluster_DBT}
(`unique_comment_id`, `other_unique_comment_id`, `score`, `diff_text`) 
VALUES ('{o_id}', '{i_id}', '{score}', NULL 
);

"""
                    else:
                        print("Everything always matches itself")


                
        
        # This will run on every outside loop
        # Save the data..
        SQLh.runQuerys(sql_dict,is_just_print)
        sql_dict = WriteOnceDict.WriteOnceDict()

        # Add the outer row to the done list
        list_of_done.append(o_id)
