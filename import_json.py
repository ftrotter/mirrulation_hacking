# A demo of the local libraries

from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh
import os
import json
import sys
import sqlalchemy as db


from io import StringIO
from html.parser import HTMLParser


# From https://stackoverflow.com/a/925630/144364
class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()




def import_json_files(directory,commentDBT):

    SQLh = mySQLh.mySQLh()
    json_objects = []
    if not os.path.exists(directory):
        print(f"The directory {directory} does not exist.")
        return json_objects

    meta_data = db.MetaData(bind=SQLh._engine)
    db.MetaData.reflect(meta_data)

    commentTable = meta_data.tables['comment']

    
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r') as file:
                    data = json.load(file)
                    commentId = data['data']['id']

                    comment_text = strip_tags(data['data']['attributes']['comment'])
                    comment_text = ' '.join(comment_text.split()) #Clever whitespace removal from https://stackoverflow.com/a/2077944/144364


                    #WE doing this the SQL alchemy way because there are gonna be hella strange text...
                    stmt = db.insert(commentTable).values(
                        commentId = commentId,
                        comment_on_documentId = data['data']['attributes']['commentOnDocumentId'],
                        comment_text = comment_text,
                        comment_date_text = data['data']['attributes']['modifyDate'],
                        ).prefix_with('IGNORE')

                    print(f"Inserting {commentId}")
                    SQLh._conn.execute(stmt)
#                    SQLh._conn.commit()
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file {filepath}")
            except db.exc.SQLAlchemyError as e:
                print(f"Tried and got\n {e}")
            except Exception as e:
                print(f"Error with file {filepath}: {e}")




if __name__ == "__main__":

    m_db = 'mirrulation'
    comment_table = 'comment'
    commentDBT = myDBTable.myDBTable(m_db,comment_table)


    if len(sys.argv) < 2:
        print("Usage: python import_json.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    import_json_files(directory_path,commentDBT)



