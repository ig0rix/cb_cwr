# coding=UTF-8

__author__ = 'igor'

import psycopg2
import sys
import json
import datetime
from database_definition import TABLES_DDL, VIEWS_DDL, FOREIGN_KEYS, INDEXES,OTHER_SCRIPTS

#Define our connection string
CONN_STRING = "host='localhost' dbname='crunchbase' user='postgres' password='postgres'"
ENTITY_LIMIT = " and (last_revision is not null)"
#ENTITY_LIMIT = ' and (last_revision is not null) and id=91032'
#ENTITY_LIMIT = ' and (last_revision is not null) LIMIT 3000'
categories = dict()
countries = dict()
product_stages = dict()
funding_round_types = dict()
currencies = dict()

def dropTables(conn):
    cursor = conn.cursor()
    try:
        #for key in VIEWS_DDL:
        #    try:
        #        cursor.execute("DROP VIEW %s" % key)
        #        conn.commit()
        #    except Exception,e:
        #        print 'Error on ddl execution: %s' % e
        #        conn.rollback()
        #
        #print '     Views droped!'

        for key in TABLES_DDL:
            try:
                cursor.execute("DROP TABLE %s CASCADE" % key)
                conn.commit()
            except Exception,e:
                print 'Error on ddl execution: %s' % e
                conn.rollback()

        print '     Tables droped!'
    finally:
        cursor.close()

def createTables(conn):
    cursor = conn.cursor()
    try:
        for key in TABLES_DDL:
            try:
                cursor.execute(TABLES_DDL[key])
                conn.commit()
            except Exception, e:
                print 'Error on ddl execution: %s' % e
                conn.rollback()

        print '     Tables created!'
    finally:
        cursor.close()

def init(conn):
    c = conn.cursor()

    c.execute("select id, name from categories order by id")
    for r in c:
        categories[r[0]] = r[1]

    c = conn.cursor()
    c.execute("select id, name from countries order by id")
    for r in c:
        countries[r[0]] = r[1]

    c.execute("select id, name from product_stages order by id")
    for r in c:
        product_stages[r[0]] = r[1]

    c.execute("select id, name from funding_round_types order by id")
    for r in c:
        funding_round_types[r[0]] = r[1]

    c.execute("select id, name from currencies order by id")
    for r in c:
        currencies[r[0]] = r[1]

    c.close()

def finalize(conn):
    cursor = conn.cursor()
    try:
        for key in VIEWS_DDL:
            try:
                cursor.execute(VIEWS_DDL[key])
                conn.commit()
            except Exception, e:
                print 'Error on views ddl execution: %s' % e
                conn.rollback()

        print '     Views created!'

        for key in FOREIGN_KEYS:
            try:
                cursor.execute(FOREIGN_KEYS[key])
                conn.commit()
            except Exception, e:
                print 'Error on FOREIGN_KEYS script execution: %s' % e
                conn.rollback()
        print '     FOREIGN_KEYS executed!'

        for key in INDEXES:
            try:
                cursor.execute(INDEXES[key])
                conn.commit()
            except Exception, e:
                print 'Error on INDEXES script execution: %s' % e
                conn.rollback()
        print '     INDEXES executed!'

        for key in OTHER_SCRIPTS:
            try:
                cursor.execute(OTHER_SCRIPTS[key])
                conn.commit()
            except Exception, e:
                print 'Error on OTHER_SCRIPTS script execution: %s' % e
                conn.rollback()
        print '     OTHER_SCRIPTS executed!'
    finally:
        cursor.close()


def addNewCategory(conn, category_name):
    id = None
    c = conn.cursor()
    try:
        c.execute("INSERT INTO categories (name) VALUES (%s) RETURNING id",[category_name])
        id = c.fetchone()[0]
        categories[id] = category_name
    except Exception,e:
        print 'Error on create new Category: %s' % e

    return id

def addNewCountry(conn, country_name):
    id = None
    c = conn.cursor()
    try:
        c.execute("INSERT INTO countries (name) VALUES (%s) RETURNING id",[country_name])
        id = c.fetchone()[0]
        countries[id] = country_name
    except Exception,e:
        print 'Error on create new country: %s' % e

    return id

def addNewStage(conn, stage_name):
    id = None
    c = conn.cursor()
    try:
        c.execute("INSERT INTO product_stages (name) VALUES (%s) RETURNING id",[stage_name])
        id = c.fetchone()[0]
        product_stages[id] = stage_name
    except Exception,e:
        print 'Error on create new Stage: %s' % e

    return id

def addNewRound(conn, round_name):
    id = None
    c = conn.cursor()
    try:
        c.execute("INSERT INTO funding_round_types (name) VALUES (%s) RETURNING id",[round_name])
        id = c.fetchone()[0]
        funding_round_types[id] = round_name
    except Exception,e:
        print 'Error on create new Category: %s' % e

    return id

def addNewCurrency(conn, currency_name):
    id = None
    c = conn.cursor()
    try:
        c.execute("INSERT INTO currencies (name,code) VALUES (%s,%s) RETURNING id",[currency_name,currency_name])
        id = c.fetchone()[0]
        currencies[id] = currency_name
    except Exception,e:
        print 'Error on create new Category: %s' % e

    return id

def jsonIntToDbInt(v):
    if v is None:
        return None
    else:
        strV = str(v)
        if strV=='' or strV=='None':
            return None
        else:
            return int(strV)

def jsonIntToDbReal(v):
    if v is None:
        return None
    else:
        strV = str(v)
        if strV=='' or strV=='None':
            return None
        else:
            return float(strV)

def jsonStrToDbStr(v):
    if v is None:
        return None
    else:
        #strV = str(v)
        strV = v.encode('utf-8')
        if strV=='':
            return None
        else:
            return strV

#TODO: change implementation
def jsonUtcToDbTimestampStr(v):
    if v is None:
        return None
    else:
        if v=='':
            return None
        else:
            return v

def jsonYMDToDate(y,m,d):
    if (y is None)  or (y==0) or (y==''):
        return None

    if (m is None)  or (m==0) or (m==''):
        m = 1
    if (d is None)  or (d==0) or (d==''):
        d = 1

    try:
        result = datetime.date(y,m,d)
    except:
        result = None

    return  result

def parseCompany(conn, id, jsonData):
    c_i = conn.cursor()
    try:
        foundingRounds = jsonData['funding_rounds']
        foundingRoundsCount = len(foundingRounds)
        companyOffices = jsonData['offices']

        c_i.execute(
            """
            INSERT INTO companies(id,name,permalink)
            VALUES(%s,%s,%s)
            """
            ,[id,jsonStrToDbStr(jsonData['name']),jsonStrToDbStr(jsonData['permalink'])]
        )
        conn.commit()
        #category
        category_id = ''
        category_code = jsonStrToDbStr(jsonData['category_code'])
        if category_code is None:
            category_code = 'other'
        for i, cat in categories.iteritems():
            if cat == category_code:
                category_id = i
        if category_id == '':
            category_id = addNewCategory(conn,category_code)

        #company country
        country_id = ''
        if len(companyOffices)>0:
            country_code = jsonStrToDbStr(companyOffices[0]['country_code'])
        else:
            country_code = 'Unknown'
        for i, cntry in countries.iteritems():
            if cntry == country_code:
                country_id = i
        if country_id == '':
            country_id = addNewCountry(conn,country_code)

        foundingRoundsAmount = 0
        for r in foundingRounds:
            if r['raised_amount'] is not None:
                foundingRoundsAmount += int(r['raised_amount'])

        #company data
        sql_str =  """
            INSERT INTO companies_data(
                id,category_id,country_id,country_name,crunchbase_url,homepage_url,blog_url,blog_feed_url,twitter_username,number_of_employees
                ,founded_year, founded_month,founded_day,founded_date
                ,deadpooled_year, deadpooled_month,deadpooled_day,deadpooled_date
                ,email_address,phone_number,description,
                created_at,updated_at,overview,total_money_raised,total_money_raised_calculated,funded_rounds_count)
            VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        params = [
                id,category_id,country_id,country_code,jsonStrToDbStr(jsonData['crunchbase_url']),jsonStrToDbStr(jsonData['homepage_url']),jsonStrToDbStr(jsonData['blog_url'])
                ,jsonStrToDbStr(jsonData['blog_feed_url']),jsonStrToDbStr(jsonData['twitter_username']),jsonIntToDbInt(jsonData['number_of_employees'])
                ,jsonIntToDbInt(jsonData['founded_year']),jsonIntToDbInt(jsonData['founded_month']),jsonIntToDbInt(jsonData['founded_day'])
                ,jsonYMDToDate(jsonData['founded_year'],jsonData['founded_month'],jsonData['founded_day'])
                ,jsonIntToDbInt(jsonData['deadpooled_year']),jsonIntToDbInt(jsonData['deadpooled_month']),jsonIntToDbInt(jsonData['deadpooled_day'])
                ,jsonYMDToDate(jsonData['deadpooled_year'],jsonData['deadpooled_month'],jsonData['deadpooled_day'])
                ,jsonStrToDbStr(jsonData['email_address']),jsonStrToDbStr(jsonData['phone_number']),jsonStrToDbStr(jsonData['description'])
                ,jsonUtcToDbTimestampStr(jsonData['created_at']),jsonUtcToDbTimestampStr(jsonData['updated_at'])
                ,jsonStrToDbStr(jsonData['overview']),jsonStrToDbStr(jsonData['total_money_raised']),foundingRoundsAmount,str(foundingRoundsCount)
        ]
        #c_i.mogrify(sql_str, params)
        c_i.execute(sql_str, params)
        conn.commit()

        #company products
        for product in jsonData['products']:
            try:
                c_i.execute(
                    """
                    INSERT INTO companies_products(company_id,product_permalink)
                    VALUES(%s,%s)
                    """
                    ,[id,product['permalink']]
                )
                conn.commit()
            except psycopg2.IntegrityError,e:  #жуем - в данных встречаются дубликаты
                conn.rollback()

        #company offices
        for office in companyOffices:
            sql_str =  """
                INSERT INTO companies_offices(
                    company_id,description,address1,address2,zip_code
                    ,city,state_code,country_code,latitude,longitude)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
            params = [
                    id,jsonStrToDbStr(office['description']),jsonStrToDbStr(office['address1']),jsonStrToDbStr(office['address2'])
                    ,jsonStrToDbStr(office['zip_code']),jsonStrToDbStr(office['city']),jsonStrToDbStr(office['state_code'])
                    ,jsonStrToDbStr(office['country_code']),jsonIntToDbReal(office['latitude']),jsonIntToDbReal(office['longitude'])
            ]
            #c_i.mogrify(sql_str, params)
            c_i.execute(sql_str, params)
            conn.commit()

        #company providerships
        for providership in jsonData['providerships']:
            is_past = providership['is_past']
            if is_past is None:   #TODO: из-за корявых данных приходиться делать такие допущения
                is_past = False
            try:
                c_i.execute(
                    """
                    INSERT INTO companies_providerships(company_id,is_past,provider_permalink,provider_name,title)
                    VALUES(%s,%s,%s,%s,%s)
                    """
                    ,[
                        id,is_past,jsonStrToDbStr(providership['provider']['permalink'])
                        ,jsonStrToDbStr(providership['provider']['name']),jsonStrToDbStr(providership['title'])
                    ]
                )
                conn.commit()
            except psycopg2.IntegrityError,e:   #жуем так ка встречаются дубликаты
                conn.rollback()
        #parse founding rounds
        for round in foundingRounds:
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(round['raised_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)
            #identify round
            round_type_id = ''
            round_type_code = jsonStrToDbStr(round['round_code'])
            if round_type_code is None:
                round_type_code = 'unattributed'
            for i, r in funding_round_types.iteritems():
                if r == round_type_code:
                    round_type_id = i
            if round_type_id == '':
                round_type_id = addNewRound(conn,round_type_code)

            sql_str =  """
                INSERT INTO funding_rounds_data(
                    id,company_id,round_type_id,currency_id
                    ,source_url,source_description,raised_amount,raised_currency_code
                    ,funded_year, funded_month,funded_day,funded_date)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    round['id'],id,round_type_id,currency_id
                    ,jsonStrToDbStr(round['source_url']),jsonStrToDbStr(round['source_description'])
                    ,jsonIntToDbReal(round['raised_amount']),jsonStrToDbStr(round['raised_currency_code'])
                    ,jsonIntToDbInt(round['funded_year']),jsonIntToDbInt(round['funded_month']),jsonIntToDbInt(round['funded_day'])
                    ,jsonYMDToDate(round['funded_year'],round['funded_month'],round['funded_day'])
            ]
            #c_i.mogrify(sql_str, params)
            c_i.execute(sql_str, params)
            conn.commit()

            investments = round['investments']
            for investor in investments:
                if investor['financial_org'] is not None:
                    investor_type='financial_org'
                    investor_permalink = investor['financial_org']['permalink']
                elif investor['person'] is not None:
                    investor_type='person'
                    investor_permalink = investor['person']['permalink']
                elif investor['company'] is not None:
                    investor_type='company'
                    investor_permalink = investor['company']['permalink']
                else:
                    print '     Warning: funding round without investor. Company id=%s' % id
                    continue
                sql_str =  """
                    INSERT INTO funding_rounds_investors(company_id,round_id,investor_permalink,investor_type)
                    VALUES(%s,%s,%s,%s)
                    """
                params = [
                        id,round['id'],jsonStrToDbStr(investor_permalink),jsonStrToDbStr(investor_type)
                ]
                #c_i.mogrify(sql_str, params)
                try:
                    c_i.execute(sql_str, params)
                    conn.commit()
                except psycopg2.IntegrityError,e:   # Жуем - встречаются дубликаты
                    conn.rollback()

        #company investments
        investments = jsonData['investments']
        for investment in investments:
            round = investment['funding_round'] #TODO: ??? is investment and funding_round relation 1:1?
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(round['raised_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)
            #identify round
            round_type_id = ''
            round_type_code = jsonStrToDbStr(round['round_code'])
            if round_type_code is None:
                round_type_code = 'unattributed'
            for i, r in funding_round_types.iteritems():
                if r == round_type_code:
                    round_type_id = i
            if round_type_id == '':
                round_type_id = addNewRound(conn,round_type_code)

            sql_str =  """
                INSERT INTO investments(
                    investor_id,investor_type_id,round_type_id,currency_id
                    ,source_url,source_description,raised_amount,raised_currency_code
                    ,funded_year, funded_month,funded_day,funded_date,icompany_name,icompany_permalink)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    id,'1',round_type_id,currency_id
                    ,jsonStrToDbStr(round['source_url']),jsonStrToDbStr(round['source_description'])
                    ,jsonIntToDbReal(round['raised_amount']),jsonStrToDbStr(round['raised_currency_code'])
                    ,jsonIntToDbInt(round['funded_year']),jsonIntToDbInt(round['funded_month']),jsonIntToDbInt(round['funded_day'])
                    ,jsonYMDToDate(round['funded_year'],round['funded_month'],round['funded_day'])
                    ,jsonStrToDbStr(round['company']['name']),jsonStrToDbStr(round['company']['permalink'])
            ]
            try:
                #c_i.mogrify(sql_str, params
                c_i.execute(sql_str, params)
                conn.commit()
            except psycopg2.IntegrityError,e:  #жуем IntegrityError - т.к. в данных дубликаты по инвестициям сплош и рядом...
                conn.rollback()
        #company acquisitions
        acquisitions = jsonData['acquisitions']
        for acquisition in acquisitions:
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(acquisition['price_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)

            sql_str =  """
                INSERT INTO companies_acquisitions(
                    company_id,currency_id,price_amount
                    ,source_url,source_description,price_currency_code,term_code
                    ,acquired_year, acquired_month,acquired_day,acquired_date,acquired_company_name,acquired_company_permalink)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    id,currency_id,jsonIntToDbReal(acquisition['price_amount'])
                    ,jsonStrToDbStr(acquisition['source_url']),jsonStrToDbStr(acquisition['source_description'])
                    ,jsonStrToDbStr(acquisition['price_currency_code']),jsonStrToDbStr(acquisition['term_code'])
                    ,jsonIntToDbInt(acquisition['acquired_year']),jsonIntToDbInt(acquisition['acquired_month']),jsonIntToDbInt(acquisition['acquired_day'])
                    ,jsonYMDToDate(acquisition['acquired_year'],acquisition['acquired_month'],acquisition['acquired_day'])
                    ,jsonStrToDbStr(acquisition['company']['name']),jsonStrToDbStr(acquisition['company']['permalink'])
            ]
            try:
                #c_i.mogrify(sql_str, params)
                c_i.execute(sql_str, params)
                conn.commit()
            except psycopg2.IntegrityError,e:  #жуем - в данных встреччаются дубликаты
                conn.rollback

    except Exception, e:
        print 'Script execution error------------------------------------------------------------------------------'
        print 'Company id:%s\n %s' % (id,e)
        print '----------------------------------------------------------------------------------------------------'
        conn.rollback()

def parseProduct(conn, id, jsonData):
    c_i = conn.cursor()
    try:
        c_i.execute(
            """
            INSERT INTO products(id,name,permalink)
            VALUES(%s,%s,%s)
            """
            ,[id,jsonStrToDbStr(jsonData['name']),jsonStrToDbStr(jsonData['permalink'])]
        )
        conn.commit()

        #check stage
        stage_id = ''
        stage_code = jsonStrToDbStr(jsonData['stage_code'])
        if stage_code is None:
            stage_code = 'unknown'
        for i, st in product_stages.iteritems():
            if st == stage_code:
                stage_id = i
        if stage_id == '':
            stage_id = addNewStage(conn,stage_code)
        #product data
        sql_str =  """
            INSERT INTO products_data(
                id,stage_id,crunchbase_url,homepage_url,blog_url,blog_feed_url,twitter_username
                ,launched_year, launched_month,launched_day,launched_date
                ,created_at,updated_at
                ,overview)
            VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        params = [
                id,stage_id,jsonStrToDbStr(jsonData['crunchbase_url']),jsonStrToDbStr(jsonData['homepage_url']),jsonStrToDbStr(jsonData['blog_url'])
                ,jsonStrToDbStr(jsonData['blog_feed_url']),jsonStrToDbStr(jsonData['twitter_username'])
                ,jsonIntToDbInt(jsonData['launched_year']),jsonIntToDbInt(jsonData['launched_month']),jsonIntToDbInt(jsonData['launched_day'])
                ,jsonYMDToDate(jsonData['launched_year'],jsonData['launched_month'],jsonData['launched_day'])
                ,jsonUtcToDbTimestampStr(jsonData['created_at']),jsonUtcToDbTimestampStr(jsonData['updated_at'])
                ,jsonStrToDbStr(jsonData['overview'])
        ]
        #c_i.mogrify(sql_str, params)
        c_i.execute(sql_str, params)
        conn.commit()
    except Exception, e:
        print 'Script execution error------------------------------------------------------------------------------'
        print 'Product id:%s\n %s' % (id,e)
        print '----------------------------------------------------------------------------------------------------'
        conn.rollback()

def parseProvider(conn, id, jsonData):
    c_i = conn.cursor()
    try:
        c_i.execute(
            """
            INSERT INTO service_providers(id,name,permalink)
            VALUES(%s,%s,%s)
            """
            ,[id,jsonStrToDbStr(jsonData['name']),jsonStrToDbStr(jsonData['permalink'])]
        )
        conn.commit()

        #service provider data
        sql_str =  """
            INSERT INTO service_providers_data(
                id,crunchbase_url,homepage_url,phone_number,email_address
                ,created_at,updated_at,overview)
            VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s)
            """
        params = [
                id,jsonStrToDbStr(jsonData['crunchbase_url']),jsonStrToDbStr(jsonData['homepage_url'])
                ,jsonStrToDbStr(jsonData['phone_number']),jsonStrToDbStr(jsonData['email_address'])
                ,jsonUtcToDbTimestampStr(jsonData['created_at']),jsonUtcToDbTimestampStr(jsonData['updated_at'])
                ,jsonStrToDbStr(jsonData['overview'])
        ]
        #c_i.mogrify(sql_str, params)
        c_i.execute(sql_str, params)
        conn.commit()
    except Exception, e:
        print 'Script execution error------------------------------------------------------------------------------'
        print 'Service provider id:%s\n %s' % (id,e)
        print '----------------------------------------------------------------------------------------------------'
        conn.rollback()

def parseFinancialOrganization(conn, id, jsonData):
    c_i = conn.cursor()
    try:
        c_i.execute(
            """
            INSERT INTO fin_orgs(id,name,permalink)
            VALUES(%s,%s,%s)
            """
            ,[id,jsonStrToDbStr(jsonData['name']),jsonStrToDbStr(jsonData['permalink'])]
        )
        conn.commit()
        #organization data
        sql_str =  """
            INSERT INTO fin_orgs_data(
                id,crunchbase_url,homepage_url,blog_url,blog_feed_url,twitter_username
                ,number_of_employees,founded_year, founded_month,founded_day,founded_date
                ,email_address,phone_number
                ,description,created_at,updated_at,overview)
            VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        params = [
                id,jsonStrToDbStr(jsonData['crunchbase_url']),jsonStrToDbStr(jsonData['homepage_url']),jsonStrToDbStr(jsonData['blog_url'])
                ,jsonStrToDbStr(jsonData['blog_feed_url']),jsonStrToDbStr(jsonData['twitter_username']),jsonIntToDbInt(jsonData['number_of_employees'])
                ,jsonIntToDbInt(jsonData['founded_year']),jsonIntToDbInt(jsonData['founded_month']),jsonIntToDbInt(jsonData['founded_day'])
                    ,jsonYMDToDate(jsonData['founded_year'],jsonData['founded_month'],jsonData['founded_day'])
            ,jsonStrToDbStr(jsonData['email_address']),jsonStrToDbStr(jsonData['phone_number']),jsonStrToDbStr(jsonData['description'])
                ,jsonUtcToDbTimestampStr(jsonData['created_at']),jsonUtcToDbTimestampStr(jsonData['updated_at']),jsonStrToDbStr(jsonData['overview'])
        ]
        #c_i.mogrify(sql_str, params)
        c_i.execute(sql_str, params)
        conn.commit()

        #financial oraganization investments
        investments = jsonData['investments']
        for investment in investments:
            round = investment['funding_round'] #TODO: ??? is investment and funding_round relation 1:1?
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(round['raised_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)
            #identify round
            round_type_id = ''
            round_type_code = jsonStrToDbStr(round['round_code'])
            if round_type_code is None:
                round_type_code = 'unattributed'
            for i, r in funding_round_types.iteritems():
                if r == round_type_code:
                    round_type_id = i
            if round_type_id == '':
                round_type_id = addNewRound(conn,round_type_code)

            sql_str =  """
                INSERT INTO investments(
                    investor_id,investor_type_id,round_type_id,currency_id
                    ,source_url,source_description,raised_amount,raised_currency_code
                    ,funded_year, funded_month,funded_day,funded_date,icompany_name,icompany_permalink)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    id,'2',round_type_id,currency_id
                    ,jsonStrToDbStr(round['source_url']),jsonStrToDbStr(round['source_description'])
                    ,jsonIntToDbReal(round['raised_amount']),jsonStrToDbStr(round['raised_currency_code'])
                    ,jsonIntToDbInt(round['funded_year']),jsonIntToDbInt(round['funded_month']),jsonIntToDbInt(round['funded_day'])
                    ,jsonYMDToDate(round['funded_year'],round['funded_month'],round['funded_day'])
                    ,jsonStrToDbStr(round['company']['name']),jsonStrToDbStr(round['company']['permalink'])
            ]
            try:
                #c_i.mogrify(sql_str, params)
                c_i.execute(sql_str, params)
                conn.commit()
            except psycopg2.IntegrityError,e:  #жуем IntegrityError - т.к. в данных дубликаты по инвестициям сплош и рядом...
                conn.rollback()
        #financial oraganization funds
        funds = jsonData['funds']
        for fund in funds:
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(fund['raised_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)

            sql_str =  """
                INSERT INTO fin_orgs_funds(
                    currency_id,name,founded_year,founded_month,founded_day,founded_date
                    ,raised_amount,raised_currency_code,source_url,source_description)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    currency_id,jsonStrToDbStr(fund['name'])
                    ,jsonIntToDbInt(fund['funded_year']),jsonIntToDbInt(fund['funded_month']),jsonIntToDbInt(fund['funded_day'])
                    ,jsonYMDToDate(fund['funded_year'],fund['funded_month'],fund['funded_day'])
                    ,jsonIntToDbReal(fund['raised_amount']),jsonStrToDbStr(fund['raised_currency_code'])
                    ,jsonStrToDbStr(fund['source_url']),jsonStrToDbStr(fund['source_description'])
            ]
            #c_i.mogrify(sql_str, params)
            c_i.execute(sql_str, params)
            conn.commit()

    except Exception, e:
        print 'Script execution error------------------------------------------------------------------------------'
        print 'Company id:%s\n %s' % (id,e)
        print '----------------------------------------------------------------------------------------------------'
        conn.rollback()

def parsePerson(conn, id, jsonData):
    c_i = conn.cursor()
    try:
        c_i.execute(
            """
            INSERT INTO persons(id,first_name,last_name,permalink)
            VALUES(%s,%s,%s,%s)
            """
            ,[id,jsonStrToDbStr(jsonData['first_name']),jsonStrToDbStr(jsonData['last_name']),jsonStrToDbStr(jsonData['permalink'])]
        )
        conn.commit()

        #person data
        sql_str =  """
            INSERT INTO persons_data(
                id,crunchbase_url,homepage_url,blog_url,blog_feed_url,twitter_username
                ,birthplace,affiliation_name,born_year, born_month,born_day,born_date
                ,created_at,updated_at,overview)
            VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
        params = [
                id,jsonStrToDbStr(jsonData['crunchbase_url']),jsonStrToDbStr(jsonData['homepage_url']),jsonStrToDbStr(jsonData['blog_url'])
                ,jsonStrToDbStr(jsonData['blog_feed_url']),jsonStrToDbStr(jsonData['twitter_username']),jsonStrToDbStr(jsonData['birthplace']),jsonStrToDbStr(jsonData['affiliation_name'])
                ,jsonIntToDbInt(jsonData['born_year']),jsonIntToDbInt(jsonData['born_month']),jsonIntToDbInt(jsonData['born_day'])
                    ,jsonYMDToDate(jsonData['born_year'],jsonData['born_month'],jsonData['born_day'])
                ,jsonUtcToDbTimestampStr(jsonData['created_at']),jsonUtcToDbTimestampStr(jsonData['updated_at']),jsonStrToDbStr(jsonData['overview'])
        ]
        #c_i.mogrify(sql_str, params)
        c_i.execute(sql_str, params)
        conn.commit()

        #person investments
        investments = jsonData['investments']
        for investment in investments:
            round = investment['funding_round'] #TODO: ??? is investment and funding_round relation 1:1?
            #TODO: refactor
            #identify currency
            currency_id = ''
            currency_code = jsonStrToDbStr(round['raised_currency_code'])
            if currency_code is None:
                currency_code = 'other'
            for i, cur in currencies.iteritems():
                if cur == currency_code:
                    currency_id = i
            if currency_id == '':
                currency_id = addNewCurrency(conn,currency_code)
            #identify round
            round_type_id = ''
            round_type_code = jsonStrToDbStr(round['round_code'])
            if round_type_code is None:
                round_type_code = 'unattributed'
            for i, r in funding_round_types.iteritems():
                if r == round_type_code:
                    round_type_id = i
            if round_type_id == '':
                round_type_id = addNewRound(conn,round_type_code)

            sql_str =  """
                INSERT INTO investments(
                    investor_id,investor_type_id,round_type_id,currency_id
                    ,source_url,source_description,raised_amount,raised_currency_code
                    ,funded_year, funded_month,funded_day,funded_date,icompany_name,icompany_permalink)
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

            params = [
                    id,'3',round_type_id,currency_id
                    ,jsonStrToDbStr(round['source_url']),jsonStrToDbStr(round['source_description'])
                    ,jsonIntToDbReal(round['raised_amount']),jsonStrToDbStr(round['raised_currency_code'])
                    ,jsonIntToDbInt(round['funded_year']),jsonIntToDbInt(round['funded_month']),jsonIntToDbInt(round['funded_day'])
                    ,jsonYMDToDate(round['funded_year'],round['funded_month'],round['funded_day'])
                    ,jsonStrToDbStr(round['company']['name']),jsonStrToDbStr(round['company']['permalink'])
            ]
            try:
                #c_i.mogrify(sql_str, params)
                c_i.execute(sql_str, params)
                conn.commit()
            except psycopg2.IntegrityError,e:  #жуем IntegrityError - т.к. в данных дубликаты по инвестициям сплош и рядом...
                conn.rollback()

    except Exception, e:
        print 'Script execution error------------------------------------------------------------------------------'
        print 'Company id:%s\n %s' % (id,e)
        print '----------------------------------------------------------------------------------------------------'
        conn.rollback()

def parseData(conn):
    c = conn.cursor()

    #service providers
    c.execute("select id,details from cwr_entities where entity_type='service-provider' %s" % ENTITY_LIMIT)
    rows = c.fetchall()
    for r in rows:
        id = r[0]
        if r[1] is None: continue
        try:
            data = json.loads(r[1], strict=False)
            parseProvider(conn,id,data)
        except Exception,e:
            print '     Load service provider(id: %s) json exception.' % id
    #persons
    c.execute("select id,details from cwr_entities where entity_type='person' %s" % ENTITY_LIMIT)
    rows = c.fetchall()
    for r in rows:
        id = r[0]
        if r[1] is None: continue
        try:
            data = json.loads(r[1], strict=False)
            parsePerson(conn,id,data)
        except Exception,e:
            print '     Load person(id: %s) json exception.' % id
     #financial organizations
    c.execute("select id,details from cwr_entities where entity_type='financial-organization' %s" % ENTITY_LIMIT)
    rows = c.fetchall()
    for r in rows:
        id = r[0]
        if r[1] is None: continue
        try:
            data = json.loads(r[1], strict=False)
            parseFinancialOrganization(conn,id,data)
        except Exception,e:
            print '     Load financial organization(id: %s) json exception.' % id
    #companies
    c.execute("select id,details from cwr_entities where entity_type='company' %s" % ENTITY_LIMIT)
    rows = c.fetchall()
    for r in rows:
        id = r[0]
        if r[1] is None: continue
        try:
            data = json.loads(r[1], strict=False)
            parseCompany(conn,id,data)
        except Exception,e:
            print '     Load company(id: %s) json exception.' % id
    #products
    c.execute("select id,details from cwr_entities where entity_type='product' %s" % ENTITY_LIMIT)
    rows = c.fetchall()
    for r in rows:
        id = r[0]
        if r[1] is None: continue
        try:
            data = json.loads(r[1], strict=False)
            parseProduct(conn,id,data)
        except Exception,e:
            print '     Load product(id: %s) json exception.' % id


def main():
    # print the connection string we will use to connect
    print "Connecting to database	-> %s" % (CONN_STRING)
    # get a connection, if a connect cannot be made an exception will be raised here
    try:
        conn = psycopg2.connect(CONN_STRING)
        try:
            dropTables(conn)
            createTables(conn)
            init(conn)
            parseData(conn)
            finalize(conn)
        finally:
            conn.close()
    except Exception, e:
        print "Connection error:\n", e

if __name__ == '__main__':
    print 'Started at %s #########################################' % str(datetime.datetime.now())
    main()
    print 'Finished at %s #########################################' % str(datetime.datetime.now())
