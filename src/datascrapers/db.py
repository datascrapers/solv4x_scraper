#from dataclasses import dataclass

import os

import firebase_admin
import pandas as pd

class DB():
  def __init__(self, data=None):
    self.data = data
    self.db   = self.init_db()

  def init_db(self):
    cred_file_path = os.path.dirname(os.path.realpath(__file__))
    cred = firebase_admin.credentials.Certificate(f'{cred_file_path}{os.sep}cred.json')
    firebase_admin.initiaize_app(cred)

    return firebase_admin.firestore.client()

  def upload_data():
    if self.data is None:
      raise Exception('no data supplied')

    if isinstance(self.db, pd.DataFrame):
      for i, row in df.iterrows():
        print(f'i: {i} row: {row}')
    else:
      print(f'self.db type: {type(self.db)}')
