# A demo of the local libraries

from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh
import os
import openai
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from datetime import datetime
import json

# Load environment variables from .env file
load_dotenv()
openai.api_key = os.getenv('OPENAI_SECRET_KEY')

MYSQL_HOST = os.getenv('MYSQL_HOST','localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT',3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

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
        response = openai.Completion.create(
            model="gpt-3.5-turbo",
            prompt=all_questions,
            max_tokens=1500,
            n=1,
            stop=None,
            temperature=0.7,
            response_format='json_object'
        )
        return response.choices[0].text.strip()
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return ""

# Retrieve comments and questions
comments = session.query(Comment).all()
questions = session.query(QuestionsForLLM).all()

# Create a prompt
for comment in comments:
    prompt_parts = []
    for question in questions:
        prompt_parts.append(f"Q: {question.question_text}\nComment: {comment.comment_text}")

    full_prompt = "\n\n".join(prompt_parts)

    # Make a single API call
    answer = ask_chatgpt(full_prompt)

    if answer:
        # Process the answer
        answers = answer.split("\n\n")
        for ans in answers:
            if ans.strip():  # Ensure non-empty answer
                new_reply = LLMReplyPerComment(
                    answer=ans,
                    commentId=comment.commentId
                )
                session.add(new_reply)

# Commit changes to the database
session.commit()

print("Processing complete. Answers saved to the database.")





