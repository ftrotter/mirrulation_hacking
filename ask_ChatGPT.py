# A demo of the local libraries

from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh
import os
from openai import OpenAI
from openai.types.chat.completion_create_params import ResponseFormat

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST','localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT',3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
client = OpenAI(api_key=os.getenv('OPENAI_SECRET_KEY'))


# SQLAlchemy setup
DATABASE_URI = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}'
print(f"trying with\n{DATABASE_URI}")
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define the tables
class Comment(Base):
    __tablename__ = 'comment'
    id = Column(Integer, primary_key=True)
    commentId = Column(String(100), nullable=False)
    comment_on_documentId = Column(String(50), nullable=False)
    comment_date_text = Column(String(50), nullable=False)
    comment_date = Column(DateTime)
    comment_text = Column(Text, nullable=False)

class LLMReplyPerComment(Base):
    __tablename__ = 'llm_reply_per_comment'
    id = Column(Integer, primary_key=True)
    question_id = Column(Integer)
    answer = Column(String(1000), nullable=False)
    commentId = Column(String(100), nullable=False)
    chatbot_id = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

class QuestionsForLLM(Base):
    __tablename__ = 'questions_for_llm'
    id = Column(Integer, primary_key=True)
    question_text = Column(String(1000), nullable=False)

# Function to call OpenAI API
def ask_chatgpt(all_questions):
    try:
        response_format = ResponseFormat(type="json_object")
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{
                    "role": "user",
                    "content": all_questions
            }],
            max_tokens=1500,
            n=1,
            stop=None,
            temperature=0.7,
            response_format=response_format
            )
        
        #Lets watch to make sure it gets the right answers
        is_debug_answers = True
        if(is_debug_answers):
            print(all_questions)
            print(completion.choices[0].message.content)

        return(completion.choices[0].message.content)
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return ""

# Retrieve comments and questions
comments = session.query(Comment).all()
questions = session.query(QuestionsForLLM).all()

#Make a list of questions
question_num = 1
questions_text = ""
for question in questions:
    questions_text += f"Q{question.id}: {question.question_text}\n"
    question_num = question_num + 1

questions_text = f"""Please answer these questions about the preceeding comment. 
Respond with a JSON object with the question number as keys. 

{questions_text}
"""


# Create a prompt
for comment in comments:

    full_prompt = f"""
What follows is a comment on a proposed regulation from the United States FDA/DEA to lower the drug schedule 
of Marijuana so that it can be prescribed for patients in the Unites States. 

Comment:
{comment.comment_text}

Questions:
{questions_text}

"""


    # Make a single API call
    answer = ask_chatgpt(full_prompt)

    if answer:
        # Process the answer
        answers_obj = json.loads(answer)
        for question_num_string, ans in answers_obj.items():
            #question_num_string will be like Q1 or Q2 etc.
            question_num = question_num_string[1:]
            question_num = int(question_num)

            new_reply = LLMReplyPerComment(
                question_id=question_num,
                answer=ans,
                commentId=comment.commentId
            )

            session.add(new_reply)
            # Commit changes to the database
            session.commit()



print("Processing complete. Answers saved to the database.")





