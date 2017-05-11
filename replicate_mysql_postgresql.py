#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    DeleteRowsEvent,
    UpdateRowsEvent
)
from configparser import SafeConfigParser
from configparser import NoSectionError
import pymysql
import socket
import calendar
import time
from datetime import datetime
from sys import exit
import psycopg2
from psycopg2 import IntegrityError
from psycopg2 import OperationalError
import sys
import os

MYSQL_SETTINGS = None

repLogFile = None
repLogPosition = None
repLogConfig = SafeConfigParser()

repHost = None
repPort = None
repUser = None
repPasswd = None
config = SafeConfigParser()

def openDestination():
  global destHost
  global destUser
  global destPasswd
  global destDb
  global destPort
  db = psycopg2.connect(host=destHost, user=destUser, password=destPasswd,dbname=destDb,port=int(destPort),application_name=os.path.basename(sys.argv[0]))
  cursor = db.cursor()
  print("Destination opened.")
  return cursor

def saveReplogPosition(repLogFile,repLogPosition):
    repLogConfig.set('replicationlog','file',repLogFile)
    repLogConfig.set('replicationlog','position',str(repLogPosition))
    with open('replogposition.ini', 'w') as f:
      repLogConfig.write(f)

def insertMessage(cursor,message):
  sql="INSERT INTO messages (author,\"from\",message) VALUES ('REPLICATION',localtimestamp,%s) RETURNING message_id"
  cursor.execute(sql,(message,))
  id=cursor.fetchone()[0]
  return id

def main():
  global repLogFile
  global repLogPosition
  global repLogConfig
  id=None

  try:
    print("Start")
    while True:
      try:
        stream = BinLogStreamReader(
            connection_settings=MYSQL_SETTINGS,
            server_id=repServerId, #server id needs to be unique
            only_events=[WriteRowsEvent,DeleteRowsEvent,UpdateRowsEvent],
            blocking=True,
            log_file=repLogFile,
            log_pos=repLogPosition,
            resume_stream=False if repLogPosition==None else True)
        print("Binlog stream opened")
        cursor=openDestination()

        for binlogevent in stream:
          if id!=None: #there has been an error
            #update error as ended
            cursor.execute("UPDATE messages SET \"to\"=localtimestamp WHERE message_id=%s",(id,))
            id=None
          #put replication log file and position in variables so we can save them later
          repLogFile = stream.log_file
          repLogPosition = stream.log_pos
          #this is the data we are interested in
          if binlogevent.schema == "weather" and binlogevent.table == "data":
            for row in binlogevent.rows:
              #we only care about inserts
              if isinstance(binlogevent, WriteRowsEvent):
                while True: #loop until we are succesfull in inserting
                  try:
                    vals = row["values"]
                    cursor.execute("INSERT INTO data (sensorid,time,value) VALUES (%s,date_trunc('minute',%s::timestamp), trunc(%s,2))",(vals["sensorid"],str(vals["time"]), str(vals["value"])))
                    cursor.execute("commit")
                    break
                  except IntegrityError as ie:
                    print("IntegrityError: " + str(sys.exc_info()[1]))
                    print("Values: "+str(vals["sensorid"]), str(vals["time"]), str(vals["value"]))
                    cursor.execute("ROLLBACK")
                    break
                  except OperationalError as e:
                    cursor=None
                    print(str(e))
                    print('Reconnect after 1 minute')
                    while True: #loop until we get a new connection
                      time.sleep(60)
                      try:
                        cursor=openDestination()
                        print('Reconnect succesfull.')
                      except:
                        print('.',)
                      if cursor != None:
                        break
        if id==None:
          id=insertMessage(cursor,"Replikointivirhe.")
        print("Binlog events ended.")
        print("Reconnect after 1 minute.")
        time.sleep(60)
      except pymysql.err.OperationalError as cre:
        if id==None:
          id=insertMessage(cursor,"Replikointivirhe.")
        print("Unable to connect: "+str(cre))
        print("Reconnect after 1 minute.")
        time.sleep(60)

  except KeyboardInterrupt:
    #close open connections
    stream.close()
    #save replication log position
    saveReplogPosition(repLogFile,repLogPosition)
  except Exception as e:
    saveReplogPosition(repLogFile,repLogPosition)
    print("Error: "+str(e))
    print("Values: "+str(vals["sensorid"]), str(vals["time"]), str(vals["value"]))
    raise e


if __name__ == "__main__":
    repLogConfig.read('replogposition.ini')
    try:
      repLogFile=repLogConfig.get('replicationlog','file')
      repLogPosition=repLogConfig.getint('replicationlog','position')
    except NoSectionError:
      repLogConfig.add_section('replicationlog')
    print('replicationlogfile ' + str(repLogFile))
    print('replicationlogposition ' + str(repLogPosition))
    config.read('replicate_mysql_postgresql.ini')
    try:
      repHost = config.get('replication_connection','host')
      repPort = config.getint('replication_connection','port')
      repUser = config.get('replication_connection','user')
      repPasswd = config.get('replication_connection','passwd')
      MYSQL_SETTINGS = {
        "host": repHost,
        "port": repPort,
        "user": repUser,
        "passwd": repPasswd
      }
      repServerId = config.getint('replication_connection','serverid')
      destHost = config.get('postgresql_config','host')
      destPort = config.getint('postgresql_config','port')
      destUser = config.get('postgresql_config','user')
      destPasswd = config.get('postgresql_config','passwd')
      destDb = config.get('postgresql_config','db')

    except NoSectionError:
      print('Error in mysql_to_graphite.ini')
      exit()
    main()

