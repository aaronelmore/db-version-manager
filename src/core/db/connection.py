import psycopg2
import re
import igraph
import os.path
import string
import random
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
    self.table = 'metatest' # Test version table
    self.create_table = 'CREATE TABLE %s (id int, name varchar(50), state varchar(20), salary int)'       
    self.insert_table = "INSERT INTO %s values (%s, '%s', '%s', %s )"
    
  def create_repo(self, repo):
    return self.backend.create_repo(repo=repo)

  def list_repos(self):
    return self.backend.list_repos()

  def delete_repo(self, repo, force=False):
    return self.backend.delete_repo(repo=repo, force=force)

  def reset(self):
    os.path.dirname(os.path.realpath(__file__))
    get_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' and table_name like '%s%s';" % (self.table,'%')
    for table in self.backend.execute_sql(get_tables_query)['tuples']:      
      print "dropping table : %s" % table
      self.backend.execute_sql("drop table %s" % table)
    base = self.GRAPH_PICKLE_BASE.split('%s')[0]
    for _file in [f for f in os.listdir('.') if os.path.isfile(f) and base in f]:
      print "removing file %s" % _file
      os.remove(_file)
    for s in self.list_repos()['tuples']:
      print "dropping : %s" % s[0]
      self.delete_repo(s[0],True)
    

#############################################################
### Versioning  
#############################################################

  def check_key_conflict(self, t1, t2):
    query = "select t1.id from %s t1 inner join %s t2 on t1.id = t2.id" % (t1,t2)
    print query
    res = self.backend.execute_sql(query)
    print res
    if res['row_count'] > 0:
      print "Cannot automeger"
    else: 
      print "No conflicts"

  def merge(self, branch, merging_branch):
    if not self.current_repo:
      raise Exception("No active repo set")
    try:
      branch = self.g.vs.find(branch)
      merging_branch = self.g.vs.find(merging_branch)
      b_out_tables = self.get_out_links_map(branch, 'table','table_name')
      m_out_tables = self.get_out_links_map(merging_branch, 'table', 'table_name')
      if b_out_tables.keys() == m_out_tables.keys():
        print "Merging"
        print "TODO trace to ancestor and find deltas" #TODO
        for table in b_out_tables.keys():
          print "Checking %s" % table
          self.check_key_conflict(b_out_tables[table]['real_table'], m_out_tables[table]['real_table'])
          
      else:
        raise NotImplementedError("Tables of branches are different.")
      
      
    except ValueError,e:
      raise ValueError('branch does not exist. %s' % e)      

  def create_fork(self, source_branch, new_branch):
    if not self.current_repo:
      raise Exception("No active repo set")
    try:
      self.g.vs.find(new_branch)
      raise ValueError('Branch %s already exists' % new_branch)
    except ValueError,e:
      try:
        _source_node = self.g.vs.find(name=source_branch, vtype='branch')
        self.g.add_vertex(new_branch, vtype='branch')
        self.g.add_edge(new_branch, source_branch)
        for table in [x for x in _source_node.neighbors(mode='OUT') if x['vtype'] == 'table']:
          table['copy_on_write'] = True
          self.g.add_edge(new_branch,table['name'])
      except ValueError,e:
        raise ValueError('Source branch %s does not exist' % source_branch)      

  def get_current_branch(self):
    if not self.current_repo:
      raise Exception("No active repo set")   
    return self.current_branch
  
  def get_all_branches(self):
    if not self.current_repo:
      raise Exception("No active repo set")
    branches = [" * %s" % branches['name'] if branches['name'] == self.current_branch else "   %s" % branches['name'] for branches in self.g.vs.select(vtype="branch")]
    
    return "\n".join(branches)

  def set_current_branch(self, branch):
    if not self.current_repo:
      raise Exception("No active repo set")
    try:
      v = self.g.vs.find(branch)
      self.current_branch = branch
    except ValueError, e:
      raise ValueError('No branch named %s exists' % branch)

  def get_rand_string(self,n):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

  def set_current_repo(self, repo):
    if not repo or len(repo) == 0:
      raise NameError("No repo name")
    repos = [z[0] for z in self.backend.list_repos()['tuples']]
    if repo in repos:
      self.current_repo = repo 
      self.current_version_file = self.GRAPH_PICKLE_BASE % self.current_repo
      #save any current meta data
      if self.g:       
        self.g.write_pickle(self.current_version_file)
        self.g = None
        
      #Init or load version graph
      if os.path.exists(self.current_version_file):
        self.g = igraph.Graph.Read_Pickle(self.current_version_file)
      else:
        self.g = igraph.Graph(directed=True)
        self.g.add_vertex('master', vtype='branch')
        self.current_branch = 'master'
        _table = "%s%s" % (self.table, self.get_rand_string(5))
        self.backend.execute_sql(self.create_table % _table)
        self.g.add_vertex(_table, vtype='table', copy_on_write=False, frozen=False, table_name=self.table, real_table=_table)
        self.g.add_edge('master',_table)
         
    else:
      raise NameError("Repo not a valid repo. Repos : %s" % repos)

  def test_meta(self):
    return str(self.g)

  def get_out_links_map(self, node, vtype, key):
    return {x[key]:x for x in node.neighbors(mode='OUT') if x['vtype'] == vtype}


  def get_out_links(self, node, vtype):
    return [x for x in node.neighbors(mode='OUT') if x['vtype'] == vtype]

  def get_in_links(self, node, vtype):
    return [x for x in node.neighbors(mode='IN') if x['vtype'] == vtype]


  '''
  select * from test3
  union
  select * from test2 where test2.id not in (select id from test3)
  union
  select * from test1 where test1.id not in (select id from test2 union select id from test3)
  '''

  def get_read_query(self,table_chain):
    base = 'select * from %s'
    limiter = 'where %s.id not in (%s)'
    queries = []
    if len(table_chain) == 1 :
      return base % table_chain[0]
    else:
      for j,table in enumerate(table_chain):
        query = base % table
        if j > 0:
          query = "%s %s" % (query,limiter)
          excludes = self.getIds(table_chain[:j])
          query = query % (table,excludes)
        queries.append(query)
    return '\nunion\n'.join(queries)  

  def getIds(self,tables):       
    if type(tables) == list:
      return ' union '.join('select id from %s' % t for t in tables)      
    else: 
      return 'select id from %s' % tables

  def test_read(self):
    if not self.current_repo or not self.current_branch:
      raise ValueError("Repo and branch must be set")    
    _table_chain = []
    _branch = self.g.vs.find(self.current_branch)
    _tables = [x for x in _branch.neighbors(mode='OUT') if x['vtype'] == 'table' and x['table_name'] == self.table]
    if len(_tables) != 1:   
      raise Exception("Error finding table")
    #build the list of ancestor tables
    _nodes = _tables
    while _nodes:
      _n = _nodes.pop(0)
      if _n['name'] not in _table_chain:
        _table_chain.append(_n['name'])
        _nodes.extend(self.get_out_links(_n,'table'))
    read_query = self.get_read_query(_table_chain)  
    print read_query
    return self.backend.execute_sql(read_query)
    

  def test_insert(self, tid, name, state, salary):  
    if not self.current_repo or not self.current_branch:
      raise ValueError("Repo and branch must be set")
    _branch = self.g.vs.find(self.current_branch)
    _tables = [x for x in _branch.neighbors(mode='OUT') if x['vtype'] == 'table' and x['table_name'] == self.table]
    _table_to_write_to = None
    if len(_tables) == 1:
      _table = _tables[0]
      if _table['copy_on_write']:       
        #we must copy the table        
        #copy and point all inbound branches
        to_del = []
        to_add = []
        for _in_branch in [x for x in _table.neighbors(mode='IN') if x['vtype'] == 'branch']:
          #create new table
          _new_table = "%s%s" % (self.table, self.get_rand_string(5))
          self.backend.execute_sql(self.create_table % _new_table)
          self.g.add_vertex(_new_table, vtype='table', copy_on_write=False, frozen=False, table_name=self.table, real_table=_new_table)          
          to_del.append((_in_branch['name'], _table['name']))
          to_add.append((_in_branch['name'], _new_table))
          to_add.append((_new_table,_table['name']))
          if _in_branch['name'] == _branch['name']:
            _table_to_write_to = _new_table
        self.g.add_edges(to_add)
        self.g.delete_edges(to_del)
        #freeze parent
        _table['frozen'] = True
        pass
      else:
        if _table['frozen']:
          raise Exception("Table is COW and is frozen :%s " % _table)
        else:
          _table_to_write_to = _table['name']
      _insert = self.insert_table % (_table_to_write_to, tid,name,state,salary)
      self.backend.execute_sql(_insert)
    else:
      raise Exception("Error finding table: %s" % str(_tables))
      
#############################################################
### END Versioning  
#############################################################
  
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