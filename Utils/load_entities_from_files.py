# coding=UTF-8

__author__ = 'igor'

import psycopg2
import sys

#Define our connection string
CONN_STRING = "host='localhost' dbname='crunchbase' user='postgres' password='postgres'"

## Data structure
#CREATE TABLE cwr_entities
#(
#  id SERIAL,
#  entity_type TEXT NOT NULL,
#  entity_id  TEXT NOT NULL,
#  entity_permalink  TEXT NOT NULL,
#  data TEXT,
#  details TEXT,
#  last_revision INT,
#  CONSTRAINT pk_cwr_entities PRIMARY KEY (entity_type,entity_id)
#)
#WITH (
#  OIDS=FALSE
#);
#ALTER TABLE cwr_entities
#  OWNER TO postgres;
#COMMENT ON COLUMN cwr_entities.entity_type IS 'Entity type (enumerated)';
#COMMENT ON COLUMN cwr_entities.entity_id IS 'Entity identifier';
#COMMENT ON COLUMN cwr_entities.entity_permalink IS 'Entity permanent link';
#COMMENT ON COLUMN cwr_entities.data IS 'Entity description data';
#COMMENT ON COLUMN cwr_entities.details IS 'Entity json data';
#COMMENT ON COLUMN cwr_entities.last_revision IS 'Last revision number';
#
#CREATE TABLE cwr_people
#(
#  id SERIAL,
#  first_name  TEXT NOT NULL,
#  last_name  TEXT NOT NULL,
#  permalink  TEXT NOT NULL,
#  data TEXT,
#  details TEXT,
#  last_revision INT,
#  CONSTRAINT pk_cwr_people PRIMARY KEY (permalink)
#)
#WITH (
#  OIDS=FALSE
#);
#ALTER TABLE cwr_entities
#  OWNER TO postgres;
#COMMENT ON COLUMN cwr_entities.entity_type IS 'Entity type (enumerated)';
#COMMENT ON COLUMN cwr_entities.entity_id IS 'Entity identifier';
#COMMENT ON COLUMN cwr_entities.entity_permalink IS 'Entity permanent link';
#COMMENT ON COLUMN cwr_entities.data IS 'Entity description data';
#COMMENT ON COLUMN cwr_entities.details IS 'Entity json data';
#COMMENT ON COLUMN cwr_entities.last_revision IS 'Last revision number';
#
#CREATE TABLE cwr_revisions
#(
#  id  int NOT NULL,
#  date_begin TIMESTAMP,
#  date_end TIMESTAMP,
#  details TEXT,
#  CONSTRAINT pk_cwr_revisions PRIMARY KEY (id)
#)
#WITH (
#  OIDS=FALSE
#);
#ALTER TABLE cwr_revisions
#  OWNER TO postgres;
#COMMENT ON COLUMN cwr_revisions.id IS 'Revision unique identifier';
#COMMENT ON COLUMN cwr_revisions.date_begin IS 'Revision process start time';
#COMMENT ON COLUMN cwr_revisions.date_end IS 'Revision process end time';
#COMMENT ON COLUMN cwr_revisions.date_end IS 'Revision process details (JSON data)';


def load_entities(conn,file_path,entity_type):
    #load entities from file
    try:
        with open(file_path,'r') as f:
            entities = eval(f.read())
    except Exception,e:
        print "I am unable load entities from file -> %s\n" % (file_path)
        print "Error details:\n", e

    try:
        cursor = conn.cursor()
        for entity in entities:
            try:
                cursor.execute(
                    """
                    INSERT INTO cwr_entities(entity_type,entity_id,entity_permalink,data,last_revision)
                    VALUES (%(entity_type)s,%(entity_id)s,%(entity_permalink)s,%(data)s,null)
                    """
                    , {"entity_type": entity_type,"entity_id": entity['name'],"entity_permalink": entity['permalink'],"data": str(entity)}
                )
                conn.commit()
            except Exception, e:
                print "Unexpected  data error:\n", e
                conn.rollback()

    except Exception, e:
        print "Unexpected error:\n", e

def load_people(conn,file_path):
    entities = []
    try:
        with open(file_path,'r') as f:
            entities = eval(f.read())
    except Exception,e:
        print "I am unable load entities from file -> %s\n" % (file_path)
        print "Error details:\n", e

    try:
        cursor = conn.cursor()
        for entity in entities:
            try:
                cursor.execute(
                    """
                    INSERT INTO cwr_entities(entity_type,entity_id,entity_permalink,data,last_revision)
                    VALUES (%s,%s,%s,%s,null)
                    """
                    , ['person', entity['permalink'], entity['permalink'], str(entity)]
                )
                conn.commit()
            except Exception, e:
                print "Unexpected  data error:\n", e
                conn.rollback()

    except Exception, e:
        print "Unexpected error:\n", e

def main():
    # print the connection string we will use to connect
    print "Connecting to database\n	-> %s" % (CONN_STRING)
    # get a connection, if a connect cannot be made an exception will be raised here
    try:
        conn = psycopg2.connect(CONN_STRING)
        #print "Load service-providers ----------------------------------------------------------------------------------"
        #load_entities(conn,'C:\CrunchBaseData\service-providers.js','service-provider')
        #print "Load financial-organizations ----------------------------------------------------------------------------"
        #load_entities(conn,'C:\CrunchBaseData\\financial-organizations.js','financial-organization')
        #print "Load products -------------------------------------------------------------------------------------------"
        #load_entities(conn,'C:\CrunchBaseData\products.js','product')
        #print "Load companies ------------------------------------------------------------------------------------------"
        #load_entities(conn, 'C:\CrunchBaseData\companies.js','company')
        print "Load people ------------------------------------------------------------------------------------------"
        load_people(conn, 'C:\CrunchBaseData\people.js')
    except Exception, e:
        print "Error:\n", e

if __name__ == '__main__':
   main()
