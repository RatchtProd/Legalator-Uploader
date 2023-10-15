from flask import Flask, redirect, url_for, render_template, request, session, after_this_request, send_file, flash
from files.uploader import upload_files
from files.searchobj import StatObj
from files.datastore import *
from files.pinecone import *
from files.userobj import *
import logging
import os


app = Flask(__name__)
app.secret_key = "admin"

# init logging
logging.basicConfig(level=logging.INFO)
handle = "main.py"
logger = logging.getLogger(handle)

DATASTORE_PROJECT="legalator"
client = get_client(DATASTORE_PROJECT)

PINECONE_API_KEY = get_config_value(client, "PINECONE_API_KEY")
PINECONE_ENVIRONMENT = get_config_value(client, "PINECONE_ENVIRONMENT")
PINECONE_INDEX_VALUE = get_config_value(client, "PINECONE_INDEX_VALUE")

pinecone_index = get_pinecone_index(PINECONE_API_KEY, PINECONE_ENVIRONMENT, PINECONE_INDEX_VALUE)

def process_files(folder_dir, document_type, document_title):
  OPENAI_API_KEY = get_config_value(client, "OPENAI_API_KEY")
  OPENAI_ORGANIZATION = get_config_value(client, "OPENAI_ORGANIZATION")
  PINECONE_API_KEY = get_config_value(client, "PINECONE_API_KEY")
  PINECONE_ENVIRONMENT = get_config_value(client, "PINECONE_ENVIRONMENT")
  PINECONE_INDEX_VALUE = get_config_value(client, "PINECONE_INDEX_VALUE")
  EMBEDDING_MODEL = get_config_value(client, "EMBEDDING_MODEL")

  
  # iterate over files in
  # that directory
  file_list = [file.name for file in os.scandir(folder_dir) if file.is_file()  ]
  #with open("list.txt", "r") as f:
  #  file_list = f.read().split('\n')
  upload_files(document_title, file_list, document_type, PINECONE_API_KEY, PINECONE_ENVIRONMENT, PINECONE_INDEX_VALUE, EMBEDDING_MODEL, OPENAI_API_KEY, OPENAI_ORGANIZATION, DATASTORE_PROJECT, folder_dir)



@app.route("/", methods=["GET"])
def upload_page():  
  logging.info("Uploading Page")

  return render_template("upload.html")



@app.route("/upload", methods=["POST"])
def upload():  
  logging.info("Uploading Page")

  folder_path = request.form.get("folder_path")
  document_title = request.form.get("document_title")
  is_folder_exists = os.path.isdir(folder_path)
  print("folder exists: ",is_folder_exists)

  document_type = request.form.get("document_type")
  print(document_type)
  print(document_title)

  process_files(folder_dir=folder_path, document_type=document_type, document_title=document_title)

  return redirect(url_for('upload_page'))

if __name__ == "__main__":
  # webbrowser.open('http://127.0.0.1:8000')  # Go to example.com
  # set upload folder
  app.config["SESSION_TYPE"] = 'filesystem'

  # run app
  app.run(port=5000, debug=True)