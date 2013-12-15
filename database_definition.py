# coding=UTF-8

__author__ = 'igor'

TABLES_DDL = {
    'calendar':"""
            CREATE TABLE calendar
            (
              date_id timestamp NOT NULL,
              year smallint,
              month smallint,
              day smallint,
              month_name TEXT,
              day_name TEXT,
              CONSTRAINT pk_calendar PRIMARY KEY (date_id)
            )
            WITH (
              OIDS=FALSE
            );
    """
    ,'categories': """
            CREATE TABLE categories
            (
              id  SERIAL,
              name TEXT,
              CONSTRAINT pk_categories PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    ,'countries': """
            CREATE TABLE countries
            (
              id  SERIAL,
              name TEXT,
              CONSTRAINT pk_countries PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'companies': """
            CREATE TABLE companies
            (
              id  INT NOT NULL,
              name TEXT,
              permalink TEXT,
              CONSTRAINT pk_companies PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'companies_data': """
            CREATE TABLE companies_data
            (
                id  INT NOT NULL,
                category_id  INT NOT NULL,
                country_id  INT NOT NULL,
                country_name  TEXT,
                crunchbase_url TEXT,
                homepage_url TEXT,
                blog_url TEXT,
                blog_feed_url TEXT,
                twitter_username TEXT,
                number_of_employees INT,
                founded_year INT,
                founded_month INT,
                founded_day INT,
                founded_date TIMESTAMP,
                deadpooled_year INT,
                deadpooled_month INT,
                deadpooled_day INT,
                deadpooled_date TIMESTAMP,
                email_address TEXT,
                phone_number TEXT,
                description TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                overview TEXT,
                total_money_raised TEXT,
                total_money_raised_calculated int,
                funded_rounds_count INT,
                CONSTRAINT pk_companies_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'companies_products': """
            CREATE TABLE companies_products
            (
              company_id  INT NOT NULL,
              product_permalink TEXT NOT NULL,
              CONSTRAINT pk_companies_products PRIMARY KEY (company_id,product_permalink)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'companies_offices': """
            CREATE TABLE companies_offices
            (
                id  SERIAL,
                company_id  INT NOT NULL,
                description TEXT,
                address1 TEXT,
                address2 TEXT,
                zip_code TEXT,
                city TEXT,
                state_code TEXT,
                country_code TEXT,
                latitude REAL,
                longitude REAL,
                CONSTRAINT pk_companies_offices PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'companies_providerships': """
            CREATE TABLE companies_providerships
            (
              company_id  INT NOT NULL,
              is_past BOOLEAN,
              provider_permalink TEXT NOT NULL,
              provider_name TEXT NOT NULL,
              title TEXT,
              CONSTRAINT pk_companies_providerships PRIMARY KEY (company_id,is_past,provider_permalink)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'funding_round_types': """
            CREATE TABLE funding_round_types
            (
              id  SERIAL,
              name TEXT,
              CONSTRAINT pk_funding_round_types PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'currencies': """
            CREATE TABLE currencies
            (
              id  SERIAL,
              name TEXT,
              code TEXT,
              CONSTRAINT pk_currencies PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'funding_rounds_data': """
            CREATE TABLE funding_rounds_data
            (
                id INT NOT NULL,
                company_id INT NOT NULL,
                round_type_id  INT NOT NULL,
                currency_id  INT NOT NULL,
                source_url TEXT,
                source_description TEXT,
                raised_amount REAL,
                raised_currency_code TEXT,
                funded_year INT,
                funded_month INT,
                funded_day INT,
                funded_date TIMESTAMP,
                CONSTRAINT pk_funding_rounds_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'funding_rounds_investors': """
            CREATE TABLE funding_rounds_investors
            (
                company_id  INT NOT NULL,
                round_id  INT NOT NULL,
                investor_permalink  TEXT NOT NULL,
                investor_type  TEXT NOT NULL,
                CONSTRAINT pk_funding_rounds_investors PRIMARY KEY (company_id, round_id,investor_permalink)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'investments': """
            CREATE TABLE investments
            (
                investor_id INT NOT NULL,
                investor_type_id INT NOT NULL,
                round_type_id  INT NOT NULL,
                currency_id  INT NOT NULL,
                source_url TEXT,
                source_description TEXT,
                raised_amount REAL,
                raised_currency_code TEXT,
                funded_year INT,
                funded_month INT,
                funded_day INT,
                funded_date TIMESTAMP,
                icompany_name TEXT,
                icompany_permalink TEXT,
                CONSTRAINT pk_investments PRIMARY KEY (investor_id,investor_type_id,round_type_id,funded_date,icompany_permalink)
            )
            WITH (
              OIDS=FALSE
            )
    """
   , 'companies_acquisitions': """
            CREATE TABLE companies_acquisitions
            (
                company_id INT NOT NULL,
                currency_id  INT NOT NULL,
                price_amount  REAL,
                source_url TEXT,
                source_description TEXT,
                price_currency_code TEXT,
                term_code TEXT,
                acquired_year INT,
                acquired_month INT,
                acquired_day INT,
                acquired_date TIMESTAMP,
                acquired_company_name TEXT,
                acquired_company_permalink TEXT,
                CONSTRAINT pk_companies_acquisitions PRIMARY KEY (company_id,acquired_company_permalink)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'product_stages': """
            CREATE TABLE product_stages
            (
              id  SERIAL,
              name TEXT,
              CONSTRAINT pk_product_stages PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'products': """
            CREATE TABLE products
            (
              id  INT NOT NULL,
              name TEXT,
              permalink TEXT,
              CONSTRAINT pk_products PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'products_data': """
            CREATE TABLE products_data
            (
                id  INT NOT NULL,
                stage_id  INT NOT NULL,
                crunchbase_url TEXT,
                homepage_url TEXT,
                blog_url TEXT,
                blog_feed_url TEXT,
                twitter_username TEXT,
                launched_year INT,
                launched_month INT,
                launched_day INT,
                launched_date TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                overview TEXT,
                CONSTRAINT pk_products_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'service_providers': """
            CREATE TABLE service_providers
            (
              id  INT NOT NULL,
              name TEXT,
              permalink TEXT,
              CONSTRAINT pk_service_providers PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'service_providers_data': """
            CREATE TABLE service_providers_data
            (
                id  INT NOT NULL,
                crunchbase_url TEXT,
                homepage_url TEXT,
                phone_number TEXT,
                email_address TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                overview TEXT,
                CONSTRAINT pk_service_providers_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'fin_orgs': """
            CREATE TABLE fin_orgs
            (
              id  INT NOT NULL,
              name TEXT,
              permalink TEXT,
              CONSTRAINT pk_fin_orgs PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'fin_orgs_data': """
            CREATE TABLE fin_orgs_data
            (
                id  INT NOT NULL,
                crunchbase_url TEXT,
                homepage_url TEXT,
                blog_url TEXT,
                blog_feed_url TEXT,
                twitter_username TEXT,
                number_of_employees INT,
                founded_year INT,
                founded_month INT,
                founded_day INT,
                founded_date TIMESTAMP,
                email_address TEXT,
                phone_number TEXT,
                description TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                overview TEXT,
                CONSTRAINT pk_fin_orgs_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'fin_orgs_funds': """
            CREATE TABLE fin_orgs_funds
            (
                id  SERIAL,
                currency_id INT,
                name TEXT,
                founded_year INT,
                founded_month INT,
                founded_day INT,
                founded_date TIMESTAMP,
                raised_amount REAL,
                raised_currency_code TEXT,
                source_url TEXT,
                source_description TEXT,
                CONSTRAINT pk_fin_orgs_funds PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'persons': """
            CREATE TABLE persons
            (
              id  INT NOT NULL,
              first_name TEXT,
              last_name TEXT,
              permalink TEXT,
              CONSTRAINT pk_persons PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
    , 'persons_data': """
            CREATE TABLE persons_data
            (
                id  INT NOT NULL,
                crunchbase_url TEXT,
                homepage_url TEXT,
                blog_url TEXT,
                blog_feed_url TEXT,
                twitter_username TEXT,
                birthplace TEXT,
                affiliation_name TEXT,
                born_year INT,
                born_month INT,
                born_day INT,
                born_date TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                overview TEXT,
                CONSTRAINT pk_persons_data PRIMARY KEY (id)
            )
            WITH (
              OIDS=FALSE
            )
    """
}


VIEWS_DDL = {
    'v_funding_rounds_data': """
        CREATE OR REPLACE VIEW v_funding_rounds_data AS
         SELECT companies_data.country_id,
            funding_rounds_data.id,
            funding_rounds_data.company_id,
            funding_rounds_data.round_type_id,
            funding_rounds_data.currency_id,
            funding_rounds_data.source_url,
            funding_rounds_data.source_description,
            funding_rounds_data.raised_amount,
            funding_rounds_data.raised_currency_code,
            funding_rounds_data.funded_year,
            funding_rounds_data.funded_month,
            funding_rounds_data.funded_day,
            funding_rounds_data.funded_date
           FROM funding_rounds_data,
            companies_data
          WHERE funding_rounds_data.company_id = companies_data.id;
    """
    ,'v_companies_data': """
            CREATE OR REPLACE VIEW v_companies_data AS
             SELECT companies.id,companies.name,companies_data.country_id,companies_data.country_name,companies_data.category_id
               FROM companies, companies_data
              WHERE companies.id = companies_data.id;
    """
    ,'v_investors': """
            CREATE VIEW v_investors
            as
            (
                select id,name, 1 as investor_type_id,'Companies' investor_type_name from companies
                union
                select id,name, 2 as investor_type_id, 'Financial organizations' investor_type_name from fin_orgs
                union
                select id,concat(first_name,' ',last_name), 3 as investor_type_id, 'Persons' investor_type_name from persons
            )
    """
}

FOREIGN_KEYS = {
    'fk_companies_data_company': """
            ALTER TABLE companies_data
              ADD CONSTRAINT fk_companies_data_company FOREIGN KEY (id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_companies_data_category': """
            ALTER TABLE companies_data
              ADD CONSTRAINT fk_companues_data_category FOREIGN KEY (category_id) REFERENCES categories (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    #,'fk_investments_company': """
    #        ALTER TABLE companies_investments
    #          ADD CONSTRAINT fk_companies_investments_company FOREIGN KEY (company_id) REFERENCES companies (id)
    #           ON UPDATE CASCADE ON DELETE CASCADE;
    #"""
    ,'fk_investments_currency': """
            ALTER TABLE investments
              ADD CONSTRAINT fk_investments_currency FOREIGN KEY (currency_id) REFERENCES currencies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_investments_round_type': """
            ALTER TABLE investments
              ADD CONSTRAINT fk_investments_round_type FOREIGN KEY (round_type_id) REFERENCES funding_round_types(id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_companies_offices_company': """
            ALTER TABLE companies_offices
              ADD CONSTRAINT fk_companies_offices_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_companies_providerships_company': """
            ALTER TABLE companies_products
              ADD CONSTRAINT fk_companies_products_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;

            ALTER TABLE companies_providerships
              ADD CONSTRAINT fk_companies_providerships_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_companies_acquisitions_company': """
            ALTER TABLE companies_acquisitions
              ADD CONSTRAINT fk_companies_acquisitions_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_companies_acquisitions_currency': """
            ALTER TABLE companies_acquisitions
              ADD CONSTRAINT fk_companies_acquisitions_currency FOREIGN KEY (currency_id) REFERENCES currencies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_funding_rounds_data_company': """
            ALTER TABLE funding_rounds_data
              ADD CONSTRAINT fk_funding_rounds_data_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_funding_rounds_data_round_type': """
            ALTER TABLE funding_rounds_data
              ADD CONSTRAINT fk_funding_rounds_data_round_type FOREIGN KEY (round_type_id) REFERENCES funding_round_types (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_funding_rounds_data_currency': """
            ALTER TABLE funding_rounds_data
              ADD CONSTRAINT fk_funding_rounds_data_currency FOREIGN KEY (currency_id) REFERENCES currencies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    ,'fk_funding_rounds_investors_company': """
            ALTER TABLE funding_rounds_investors
              ADD CONSTRAINT fk_funding_rounds_investors_company FOREIGN KEY (company_id) REFERENCES companies (id)
               ON UPDATE CASCADE ON DELETE CASCADE;
    """
    #,'fk_funding_rounds_investors_investor_type': """
    #        ALTER TABLE funding_rounds_investors
    #          ADD CONSTRAINT fk_funding_rounds_investors_investor_type FOREIGN KEY (investor_type) REFERENCES companies (id)
    #           ON UPDATE CASCADE ON DELETE CASCADE;
    #"""
}

INDEXES = {
    'uk_companies_permalink': """
            CREATE UNIQUE INDEX uk_companies_permalink
               ON companies (permalink text_pattern_ops ASC NULLS LAST);
    """
}

OTHER_SCRIPTS = {
    'FILL_calendar': """
            INSERT INTO calendar
            select DISTINCT funded_date,funded_year,funded_month,funded_day, to_char(funded_date,'month YYYY'),to_char(funded_date,'YYYY-MM-DD') from funding_rounds_data
            where funded_date is not null and (funded_day is not null) and (funded_month is not null) and funded_year>=1960
            order by funded_date;
    """
}
