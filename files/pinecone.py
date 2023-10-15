import pinecone
import logging
import os

# pinecone
def get_pinecone_index(API_KEY, ENVIRONMENT, INDEX) -> pinecone.Index:
  pinecone.init(
    api_key=API_KEY,
    environment=ENVIRONMENT 
  )
  index = pinecone.Index(INDEX)
  return index

def get_recent_id(pinecone_index: pinecone.Index):
  return pinecone_index.describe_index_stats()['total_vector_count']

def get_statistics(pinecone_index: pinecone.Index):
  return pinecone_index.describe_index_stats()
