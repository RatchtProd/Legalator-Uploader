from files.pinecone import get_recent_id, get_pinecone_index
import pinecone
import logging
from files.read import read_txt, read_pdf
import pandas as pd
from concurrent.futures.thread import ThreadPoolExecutor
from files.datastore import create_datastore_entry, get_async_client, get_client
import openai
import tqdm
import concurrent.futures
import os
import files.decorators as decorators

def batch_uploader(client, ids, texts, types, titles):
  executor = ThreadPoolExecutor()

  futures = [executor.submit(create_datastore_entry, client, ["Chunk", id], {"Text":text, "Type":type, "Document Title":title}, exclude_from_indexes=["Text"]) for id, text, type, title in zip(ids, texts, types, titles)]
  done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)


def post_files(df: pd.DataFrame, pinecone_index: pinecone.Index, datastore_client, batch_size: int = 32):
  # init datastore client

  for batch_start in range(0, len(df.index), batch_size):
    batch_end = batch_start + batch_size
    
    print(f"Batch {batch_start} to {batch_end-1}")

    batch = df[batch_start:batch_end]

    batch_titles = batch['title'].tolist()
    batch_types = batch['type'].tolist()

    startingID = get_recent_id(pinecone_index)
    batch_ids = [str(i+startingID) for i in range(0,len(batch_titles))]
    batch_embeddings = batch['embedding'].tolist()
    batch_text = batch['text'].tolist()
    batch_inner_ids = batch['inner_id'].tolist()

    batch_uploader(ids=batch_ids, texts=batch_text, types=batch_types, titles=batch_titles, client=datastore_client)
    
    meta = [{'title':title, 'inner_id':inner_id, 'type':type} for title, inner_id, type in zip(batch_titles, batch_inner_ids, batch_types)]
    
    # prep metadata and upsert batch
    to_upsert = zip(batch_ids, batch_embeddings, meta)


    # upsert to Pinecone
    try:
      pinecone_index.upsert(vectors=list(to_upsert))
    except Exception as e: 
      raise Exception(f"Pinecone Error: {e}")


def upload_batch_meta(ids, texts, types, titles, client):
  for id, text, type, title in zip(ids, texts, types, titles):
    create_datastore_entry(client, ["Chunk", id], {"Text":text, "Type":type, "Document Title":title}, exclude_from_indexes=["Text"])

# process embeddings
def create_df(chunks: list, chapter_title: str, document_title, document_type: str, embedding_model:str, batch_size = 1000) -> pd.DataFrame:
  embeddings = []
  for batch_start in tqdm(range(0, len(chunks), batch_size), ncols=80):
    batch_end = batch_start + batch_size
    batch = chunks[batch_start:batch_end]
    print(f"Batch {batch_start} to {batch_end-1}")
    try:
      response = openai.Embedding.create(model=embedding_model, input=batch)
    except Exception as e:
      raise Exception(f"Exception {e}, {batch},  {e}")
    for i, be in enumerate(response["data"]):
      assert i == be["index"]  # double check embeddings are in same order as input
    batch_embeddings = [e["embedding"] for e in response["data"]]
    embeddings.extend(batch_embeddings)

  document_titles = [document_title for n in range(0, len(embeddings))]
  document_types = [document_type for n in range(0, len(embeddings))]
  inner_id = [i for i in range(0,len(embeddings))]
  df = pd.DataFrame({"text": chunks, "embedding": embeddings, "title": document_titles, "type": document_types, "inner_id": inner_id})
  return df

#== params ==#
def upload_files(document_title, chapter_list: list[str], document_type, PINECONE_API_KEY, ENVIRONMENT, INDEX, EMBEDDING_MODEL, OPENAI_API_KEY, ORGANIZATION, DATASTORE_PROJECT, folder_dir):
  openai.api_key = OPENAI_API_KEY
  openai.organization = ORGANIZATION

  pinecone_index = get_pinecone_index(PINECONE_API_KEY, ENVIRONMENT, INDEX)
  datastore_client = get_client(DATASTORE_PROJECT)

  
  for chapter in chapter_list:
    try:
      logging.info(f"File Name: {chapter}")

      ext = os.path.splitext(chapter)[-1].lower()

      # Now we can simply use == to check for equality, no need for wildcards.
      if ext == ".txt":
        chunks = read_txt(chapter, folder_dir)
      elif ext == ".pdf":
        chunks = read_pdf(chapter, folder_dir)
      else:
        logging.error("Unknown file type detected. Make sure that the files are .txt or .pdf")
        continue
      
      df = create_df(chunks, chapter_title=chapter, document_type=document_type, document_title=document_title, embedding_model=EMBEDDING_MODEL)
      #print(df)
      post_files(df, pinecone_index, datastore_client)
    except (openai.error.APIError, openai.error.InvalidRequestError, openai.error.Timeout) as oe:
      logging.error(f"Something went wrong with the file when passing to OpenAI... {oe}")
    except Exception as e:
      logging.error(f"Something went wrong... {e}")
    finally:
      logging.info("After Upload...")