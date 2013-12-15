# coding=UTF-8

__author__ = 'igor'

import psycopg2
import json
from crunchbase import CrunchBase

#Define our connection string
CONN_STRING = "host='localhost' dbname='crunchbase' user='postgres' password='postgres'"

def start(new=False):
    """
    @rtype : int
    """
    revision_number = 0
    try:
        service_conn = psycopg2.connect(CONN_STRING)

        service_cursor = service_conn.cursor()
        if new:
            detect_revision_sql = "SELECT coalesce(max(id),0)+1 FROM cwr_revisions"
        else:
            detect_revision_sql = "SELECT coalesce(max(id),0) FROM cwr_revisions"
        service_cursor.execute(detect_revision_sql)
        revision_number = service_cursor.fetchone()[0];
        service_cursor.execute("SELECT count(1) FROM cwr_revisions")
        revisions_count = service_cursor.fetchone()[0];

        if (revisions_count == 0) or (new):
            service_cursor.execute(
                """
                INSERT INTO cwr_revisions(id,date_begin)
                VALUES (%(revision_number)s,now())
                """
                , {"revision_number": revision_number}
            )

        service_cursor.close()

        service_conn.commit()
        service_conn.close()
    except Exception, e:
        print "Service error:\n", e

    return revision_number

def stop(revision_number):
    """
    @rtype : int
    """
    try:
        service_conn = psycopg2.connect(CONN_STRING)

        service_cursor = service_conn.cursor()
        service_cursor.execute(
            """
            UPDATE cwr_revisions SET date_end=now()
            WHERE id=%s
            """
            , [revision_number]
        )
        service_cursor.close()

        service_conn.commit()
        service_conn.close()
    except Exception, e:
        print "Service error:\n", e

    return revision_number

def getData(cb,entity_id,entity_type):
    if entity_type == 'company':
        data = cb.getCompanyData(entity_id)
    elif entity_type == 'service-provider':
        data = cb.getServiceProviderData(entity_id)
    elif entity_type == 'financial-organization':
        data = cb.getFinancialOrgData(entity_id)
    elif entity_type == 'product':
        data = cb.getProductData(entity_id)
    elif entity_type == 'person':
        data = cb.getPersonData(entity_id)
    else:
        raise ValueError('Unknown entity type: %s' % entity_type)

    return data

def uploadEntity(cb,conn,id,entity_id,entity_permalink,entity_type, revision_number):
    data = getData(cb,entity_id,entity_type)
    if data is None:  #try get data by permalink
        print "Try get data by permalink"
        data = getData(cb,entity_permalink,entity_type)

    if data is not None:
        data = json.dumps(data)

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE cwr_entities SET details=%s, last_revision=%s
            WHERE id=%s
            """
            ,[data,str(revision_number),str(id)]
        )
        conn.commit()
        print '!!!: Entity %s uploaded' % str(id)
    finally:
        cursor.close()

def main():
    # print the connection string we will use to connect
    print "Connecting to database	-> %s" % (CONN_STRING)
    # get a connection, if a connect cannot be made an exception will be raised here
    try:
        conn = psycopg2.connect(CONN_STRING)
        revision_number = start()
        print "Revision started: %s" %  revision_number
        try:
            #cb = CrunchBase('fushndkwh6nhp5wytd9tmbs8')
            cb = CrunchBase('j4rufy6d4zckdka4cwxhdeda')
            entities_cursor = conn.cursor()
            entities_cursor.execute(
                #"SELECT id,entity_type,entity_id,entity_permalink FROM cwr_entities WHERE id=100026 order by id"
                #"SELECT id,entity_type,entity_id,entity_permalink FROM cwr_entities WHERE (id>0 and id<=232745) and ((last_revision is NULL) or (last_revision<%s)) order by id"
                "SELECT id,entity_type,entity_id,entity_permalink FROM cwr_entities WHERE (id>400000 and id<=410000) and ((last_revision is NULL) or (last_revision<%s)) order by id"
                #"SELECT id,entity_type,entity_id,entity_permalink FROM cwr_entities WHERE (entity_type='product') and ((last_revision is NULL) or (last_revision<%s)) order by id LIMIT 20"
                ,[revision_number]
            )
            row_count = 0
            for record in entities_cursor:
                row_count += 1
                id = record[0]
                entity_type = record[1]
                entity_id = record[2]
                entity_permalink = record[3]
                uploadEntity(cb,conn,id,entity_id,entity_permalink,entity_type,revision_number)
        except Exception, e:
            print "Error:\n", e
        finally:
            conn.close()
            stop(revision_number)
            print "Revision stoped: %s" %  revision_number
    except Exception, e:
        print "Connection error:\n", e

if __name__ == '__main__':
   main()

