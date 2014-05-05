import psycopg2
import re
import igraph
import os.path
from backend.pg import PGBackend

'''
@author: anant bhardwaj
@date: Oct 3, 2013

DataHub DB wrapper for backends (only postgres implemented)
'''

class Connection:
  GRAPH_PICKLE_BASE = 'version-graph-%s.txt'
  def __init__(self, user, password):    
    self.backend = PGBackend(user, password)
    self.current_repo = None
    self.current_version_file = None
    self.current_branch = None
    self.g = None
    
  def create_repo(self, repo):
    return self.backend.create_repo(repo=repo)

  def list_repos(self):
    return self.backend.list_repos()

  def delete_repo(self, repo, force=False):
    return self.backend.delete_repo(repo=repo, force=force)

  def create_fork(self, source_branch, new_branch):
    if not self.current_repo:
      raise Exception("No active repo set")
    try:
      self.g.vs.find(new_branch)
      raise ValueError('Branch %s already exists' % new_branch)
    except ValueError,e:
      try:
        self.g.vs.find(source_branch)
        self.g.add_vertex(new_branch, vtype='branch')
        self.g.add_edge(new_branch, source_branch)
      except ValueError,e:
        raise ValueError('Source branch %s does not exist' % source_branch)      

  def get_current_branch(self):
    return self.current_branch

  def set_current_branch(self, branch):
    if not self.current_repo:
      raise Exception("No active repo set")
    try:
      v = self.g.vs.find(branch)
      self.current_branch = branch
    except ValueError, e:
      raise ValueError('No branch named %s exists' % branch)

  def set_current_repo(self, repo):
    if not repo or len(repo) == 0:
      raise NameError("No repo name")
    repos = [z[0] for z in self.backend.list_repos()['tuples']]
    if repo in repos:
      self.current_repo = repo 
      self.current_version_file = self.GRAPH_PICKLE_BASE % self.current_repo
      #Init or load version graph
      if os.path.exists(self.current_version_file):
        self.g = igraph.Graph.Read_Pickle(self.current_version_file)
      else:
        self.g = igraph.Graph(directed=True)
        self.g.add_vertex('master', vtype='branch')
        self.current_branch = 'master'
    else:
      raise NameError("Repo not a valid repo. Repos : %s" % repos)
  
  def list_tables(self, repo):
    return self.backend.list_tables(repo=repo)

  def desc_table(self, table):
    return self.backend.desc_table(table=table)

  def execute_sql(self, query, params=None):
    return self.backend.execute_sql(query, params) 

  def close(self):    
    self.backend.close()
    self.g.write_pickle(self.current_version_file)

  '''
  The following methods run in superuser mode
  '''
  @staticmethod
  def create_user(username, password):
    s_backend = PGBackend(user='postgres', password='postgres')
    s_backend.create_user(username, password)
    s_backend.execute_sql('CREATE DATABASE %s WITH OWNER=%s' %(username, username))
    return s_backend.create_user(username, password)

  @staticmethod
  def change_password(username, password):
    s_backend = PGBackend(user='postgres', password='postgres')
    return s_backend.change_password(username, password)