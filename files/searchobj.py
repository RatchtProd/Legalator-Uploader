import json
from datetime import datetime
from files.searchtype import SearchType

class StatObj:
  def __init__(self, prompt, response, titles, type: SearchType, user, time, id):
    self.prompt = prompt
    self.type = type
    self.response = response
    self.titles = titles
    self.user = user
    self.time = datetime.fromtimestamp(time)
    self.id = id
  
  
  def jsonify(self):
    return dict(prompt = self.prompt, response=self.response, titles = self.titles, type=self.type.value, user = self.user, time = self.time, id = self.id) 
  


