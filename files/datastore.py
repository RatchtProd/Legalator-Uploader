from google.cloud import datastore, datastore_v1
import logging
import json
from flask import session




"""This is legacy. Do not use. Just keep as reference for functions"""

"""
def fetch_config_value_by_filter(key: str):
  filters = [("Type", "=", key)]
  query = client.query(kind='Config')
  query.add_filter(filter=PropertyFilter("Type", "=", key))
  res = list(query.fetch())
  if (len(res) == 0): raise Exception("Datastore: Entity not found in database")
  if (len(res) > 1): raise Exception("Datastore: You fucked up with your keys. Multiple entities were returned")
  return res
"""

def get_count(client: datastore.Client, key_type):
  query = client.query(kind="__Stat_Kind__")
  query
  result = list(query.fetch())
  for stat in result:
    if stat['kind_name'] == key_type: return stat['count']
  
  # should not be here
  raise Exception("Kind not found")

def fetch_recent_statistics(client: datastore.Client, limit=5):
  query = client.query(kind="Statistics")
  query.order = ["-Time Added"]
  results = list(query.fetch(limit=limit))

  return results

def get_users(client: datastore.Client, limit=None):
  query = client.query(kind="User")
  results = list(query.fetch(limit=limit))

  return results
  

def create_datastore_entry(client, key_list:list, value_map, exclude_from_indexes:list=[]):
  complete_key = client.key(*key_list)
  entity = datastore.Entity(key=complete_key, exclude_from_indexes=exclude_from_indexes)
  entity.update(
    value_map
  )
  client.put(entity)

def check_datastore_entry(client: datastore.Client, key_type: str, key:str) -> bool:
  keys = client.key(key_type, key)
  entity = client.get(key=keys)
  if (entity == None): return False
  return True

def get_datastore_entry(client, key_type: str, key:str):
  keys = client.key(key_type, key)
  entity = client.get(key=keys)
  if (entity == None): raise KeyError(f"Datastore: Entity not found in database. KeyType: {key_type}, Key: {key}" )
  return entity

def get_datastore_entry_id(client, key_type: str, key:int):
  keys = client.key(key_type, int(key))
  entity = client.get(key=keys)
  if (entity == None): raise KeyError(f"Datastore: Entity not found in database. KeyType: {key_type}, Key: {key}" )
  return entity


def update_datastore_entry(client, key_type: str, key:str, value_map:str):
  keys = client.key(key_type, key)
  entity = client.get(key=keys)
  if (entity == None): raise KeyError("Datastore: Entity not found in database")
  entity.update(
    value_map
  )
  client.put(entity)

def update_config_value(client, key: str, value:str):
  keys = client.key("Config", key)
  entity = client.get(key=keys)
  if (entity == None): raise KeyError("Datastore: Entity not found in database")
  entity.update(
    {
      "Value": value
    }
  )
  client.put(entity)


def get_config_value(client, key:str):
  return get_config_entity_by_key(client, key)['Value']

def get_config_entity_by_key(client, key:str):
  keys = client.key("Config", key)
  entity = client.get(key=keys)
  if (entity == None): raise KeyError("Datastore: Entity not found in database")
  return entity

def session_get_config_value(client, key: str) -> str:
  # key is datastore key
  if key.lower() not in session:
    session[key.lower()] = json.dumps(get_config_value(client, key))

  return json.loads(session[key.lower()])

def get_client(project:str):
  return datastore.Client(project=project)

def get_async_client(project:str):
  return datastore_v1.DatastoreAsyncClient(project=project)

def acreate_datastore_entry(client, key_list:list, value_map, exclude_from_indexes:list=[]):
  complete_key = client.Key(*key_list)
  entity = datastore.Entity(key=complete_key, exclude_from_indexes=exclude_from_indexes)
  entity.update(
    value_map
  )
  client.put(entity)



#try:
#  logger.info(fetch_config_value("EMBEDDING_MODEL"))
#except Exception as e:
#  logger.error(f"oh shit: {e}")

