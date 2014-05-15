import random
import string
import os
import subprocess
import shlex
import re
import logging
import time
import shutil

logger = logging.getLogger('datahub')
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler('datahub.log')
fh.setLevel(logging.ERROR)
# create console handler with a lower log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s  - %(levelname)s : %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

SERVER = 'localhost'
PG_USER = 'dbv'

####################################
# UTILS
####################################

def cleanDirs():
    shutil.rmtree(config_dir, True)
    os.mkdir(config_dir)
    shutil.rmtree(results_dir, True)
    os.mkdir(results_dir)
    
def localCmdOutput(cmd,checkStringFor=None):
  #logger.info("Calling remote cmd, blocking for output: %s " %(cmd))
  proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  stdout = proc.communicate()[0]
  res = proc.wait()
  if res == 0:
    if checkStringFor:
      if stdout.count(checkStringFor) < 1:
        raise Exception("Error: %s executed, but results did not contain %s. Result:%s" % (cmd,checkStringFor,stdout))
    return stdout.replace("DEB_HOST_MULTIARCH is not a supported variable name at /usr/bin/dpkg-architecture line 214.\n","")
  else:
   raise Exception("Error : %s on remoteCmdBlock %s. Output : %s" %(res,cmd,stdout))

def checkPGActiveDB(db):
  checkActive = "select count(*) from pg_stat_activity where \"usename\" ='ilandlord' and \"datname\"= '%s';" % (db);
  remoteCheck = "psql -U %s -h %s postgres -w -c \"%s\"" % (PG_USER,SERVER,checkActive)
  res =localCmdOutput(remoteCheck,"count")
  count = res.split('\n')[2].strip()
  if count.isdigit():
    count= int(count)
    if count > 0 :
      return True
    else:
      return False
    
  else:
    raise Exception("Error processing %s. Results: %s Checking:" % (checkActive,str(res))  )
  

def cleanPGDB(db,retryCount=4,timeToSleep=30):
    #alternative http://www.postgresql.org/docs/8.2/interactive/app-dropdb.html
    if retryCount==0:
        raise Exception("Unable to remove DB file %s on %s "% (db, SERVER))
    if not checkPGActiveDB(db):
        dbDrop = "DROP DATABASE IF EXISTS %s" % (db)
        remoteCheck = "psql -U %s -h %s postgres -w -c \"%s\"" % (PG_USER,SERVER,dbDrop)
        logger.info("dropping DB %s" %db)
        res =localCmdOutput(remoteCheck,"DROP DATABASE")
        return res
    else:
        logger.info("Sleeping for %s"%timeToSleep)
        time.sleep(timeToSleep)
        retryCount-=1
        return cleanPGDB(db,retryCount)

def cleanDBs(name_base='test'):
    dbs= getListOfPGDBsMatching(name_base)
    for db in dbs:
        cleanPGDB(db)
    
def getListOfPGDBsMatching(dbNameRegex, node=SERVER):
    listDBs = "psql -l -t -h %s -U %s" % (node,PG_USER)
    res = localCmdOutput(listDBs,"en_US.UTF-8")
    dbLines =  res.split('\n')
    dbRaw = [x.split('|')[0].strip() for x in dbLines if len(x) > 0]
    dbs = [ x for x in dbRaw if len(x) > 0 and re.search(dbNameRegex,x)]
    return dbs    