#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
#
#Graphite config is stored in the following table
#mysql> desc graphite;
#+--------------+---------------------+------+-----+---------+-------+
#| Field        | Type                | Null | Key | Default | Extra |
#+--------------+---------------------+------+-----+---------+-------+
#| sensorid     | bigint(20) unsigned | NO   | PRI | NULL    |       |
#| graphitepath | varchar(200)        | NO   |     | NULL    |       |
#| formula      | varchar(4000)       | YES  |     | NULL    |       |
#+--------------+---------------------+------+-----+---------+-------+


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

MYSQL_SETTINGS = None

CARBON_SERVER = None
CARBON_PORT = None

_EPOCH = datetime(1970, 1, 1)

repLogFile = None
repLogPosition = None
repLogConfig = SafeConfigParser()

graphiteConfHost = None
graphiteConfUser = None
graphiteConfPasswd = None
graphiteConfDb = None
repHost = None
repPort = None
repUser = None
repPasswd = None
config = SafeConfigParser()

def readGraphiteConfig():
  global graphiteConfHost
  global graphiteConfUser
  global graphiteConfPasswd
  global graphiteConfDb

#  db = pymysql.connect(host=graphiteConfHost, user=graphiteConfUser, passwd=graphiteConfPasswd, db=graphiteConfDb)
#  cursor = db.cursor()

#  cursor.execute("select sensorid,graphitepath,formula from graphite")
  # get the resultset as a tuple
#  result = cursor.fetchall()
  graphiteConfig = {}
#  for record in result:
#    rec = dict({'graphitepath':record[1],'formula':record[2]})
#    graphiteConfig[record[0]]=rec

#  cursor.close()
#  db.close()
  print("Read graphite conf.")
  return graphiteConfig

def main():
  global repLogFile
  global repLogPosition
  global repLogConfig

  graphiteConfig = readGraphiteConfig()

  try:
    print("Start")
#    sock = socket.socket()
#    sock.connect((CARBON_SERVER, CARBON_PORT))
    print('Carbon socket opened.')
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=2, #server id needs to be unique
        only_events=[WriteRowsEvent,DeleteRowsEvent,UpdateRowsEvent],
        blocking=True,
        log_file=repLogFile,
        log_pos=repLogPosition,
        resume_stream=False if repLogPosition==None else True)
    print("Binlog stream opened")

    for binlogevent in stream:
      #put replication log file and position in variables so we can save them later
      repLogFile = stream.log_file
      repLogPosition = stream.log_pos
      #also check for changes in graphite configuration and read again if needed
#      if binlogevent.schema == "weather" and binlogevent.table == "graphite":
#        graphiteConfig = readGraphiteConfig()
      #this is the data we are interested in
      print("event in "+str(binlogevent.schema)+"."+str(binlogevent.table))
      if binlogevent.schema == "weather" and binlogevent.table == "data":
        print("event type1 "+type(binlogevent).__name__)
        print("event rows "+str(binlogevent.rows))
        for row in binlogevent.rows:
          print("event type2 "+type(binlogevent).__name__)
          #we only care about inserts
          if isinstance(binlogevent, WriteRowsEvent):
            vals = row["values"]
            print(str(vals["sensorid"]), str(vals["time"]), str(vals["value"]))
            #check if the sensor is one that we have configuration for
 #           if vals["sensorid"] in graphiteConfig:
 #             conf = graphiteConfig[vals["sensorid"]]
 #             value = float(vals["value"])
 #             #do a conversion if needed
 #             if conf["formula"]!=None and conf["formula"]!="":
 #               value=eval(conf["formula"], {"__builtins__": {}}, {"value":value,"round":round})
              #construc the message and send it to carbon
 #             message = '%s %f %d\n' % (conf["graphitepath"], value, round((vals["time"] - _EPOCH).total_seconds()))
 #             sock.sendall(message)
 #             print str(vals["sensorid"]), str(vals["time"]), str(value)
 #             print message

  except KeyboardInterrupt:
    #close open connections
    stream.close()
#    sock.close()
    #save replication log position
    repLogConfig.set('replicationlog','file',repLogFile)
    repLogConfig.set('replicationlog','position',str(repLogPosition))
    with open('replogposition.ini', 'w') as f:
      repLogConfig.write(f)


if __name__ == "__main__":
    repLogConfig.read('replogposition.ini')
    try:
      repLogFile=repLogConfig.get('replicationlog','file')
      repLogPosition=repLogConfig.getint('replicationlog','position')
    except NoSectionError:
      repLogConfig.add_section('replicationlog')
    print('replicationlogfile' + str(repLogFile))
    print('replicationlogposition' + str(repLogPosition))
    config.read('replicate_mysql_postgresql.ini')
    try:
#      graphiteConfHost = config.get('graphite_config','host')
#      graphiteConfUser = config.get('graphite_config','user')
#      graphiteConfPasswd = config.get('graphite_config','passwd')
#      graphiteConfDb = config.get('graphite_config','db')
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
 #     CARBON_SERVER = config.get('carbon','host')
 #     CARBON_PORT = config.getint('carbon','port')
    except NoSectionError:
      print('Error in mysql_to_graphite.ini')
      exit()
    main()

