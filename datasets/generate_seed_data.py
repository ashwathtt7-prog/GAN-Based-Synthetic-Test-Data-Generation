"""
Seed Data Generator for POC
Uses Faker to generate realistic telecom seed data into a source SQLite database.
This simulates the real production databases we would connect to.
"""

import os
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import pandas as pd
from sqlalchemy import create_engine, text

# Initialize Faker and Logging
fake = Faker('en_US')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DB_PATH = Path('datasets/telecom_source.db')
DDL_DIR = Path('datasets/ddl')

def setup_database():
    """Create the SQLite database and run DDL scripts to create schemas."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    
    engine = create_engine(f'sqlite:///{DB_PATH}')
    
    for sql_file in DDL_DIR.glob('*.sql'):
        logger.info(f"Executing DDL: {sql_file.name}")
        with open(sql_file, 'r') as f:
            sql_statements = f.read().split(';')
            with engine.begin() as conn:
                for statement in sql_statements:
                    if statement.strip():
                        # Basic cleanup, SQLite doesn't support all VARCHAR/DECIMAL syntax but accepts it
                        conn.execute(text(statement))
    
    return engine

def generate_customers(num_records=5000):
    logger.info(f"Generating {num_records} CUSTOMERS...")
    records = []
    for i in range(1, num_records + 1):
        acq_dt = fake.date_between(start_date='-5y', end_date='today')
        records.append({
            'CUST_ID': i,
            'CUST_FRST_NM': fake.first_name(),
            'CUST_LST_NM': fake.last_name(),
            'CUST_MID_NM': fake.first_name() if random.random() > 0.5 else None,
            'CUST_DOB': fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
            'CUST_SSN': fake.ssn().replace('-', ''),
            'CUST_GNDR_CD': random.choice(['M', 'F', 'X']),
            'CUST_TEN_MNT': random.randint(1, 60),
            'CUST_SGMT_CD': random.choice(['PLATINUM', 'GOLD', 'SILVER', 'BRONZE']),
            'CUST_RISK_SCR': round(random.uniform(0, 100), 2),
            'CUST_CRED_SCR': random.randint(300, 850),
            'CUST_ACQSN_CHNL_CD': random.choice(['WEB', 'STORE', 'CALL_CENTER', 'PARTNER']),
            'CUST_ACQSN_DT': acq_dt.isoformat(),
            'CUST_STAT_CD': random.choices(['ACT', 'SUS', 'TRM', 'PDG'], weights=[0.8, 0.05, 0.1, 0.05])[0],
            'CUST_STAT_RSN_CD': 'DEFAULT',
            'CUST_LANG_PREF_CD': random.choices(['EN', 'ES', 'FR'], weights=[0.8, 0.15, 0.05])[0],
            'CUST_CREAT_DT': acq_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'CUST_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'CUST_UPDT_BY': 'SYSTEM'
        })
    return pd.DataFrame(records)

def generate_service_plans():
    logger.info("Generating SVC_PLAN_REF...")
    records = [
        {'PLAN_CD': 'VOICE_BASIC', 'PLAN_NM': 'Basic Voice Plan', 'PLAN_TYP_CD': 'VOICE', 'PLAN_TIER_CD': 'BASIC', 'PLAN_MNT_COST_AMT': 20.00, 'PLAN_DATA_LMT_GB': 0, 'PLAN_VOICE_LMT_MIN': 500, 'PLAN_SMS_LMT_CNT': 100, 'PLAN_INTL_FLG': 'N', 'PLAN_5G_FLG': 'N', 'PLAN_STRT_DT': '2020-01-01', 'PLAN_END_DT': None, 'PLAN_STAT_CD': 'ACT'},
        {'PLAN_CD': 'DATA_UNLMTD', 'PLAN_NM': 'Unlimited Data', 'PLAN_TYP_CD': 'DATA', 'PLAN_TIER_CD': 'UNLIMITED', 'PLAN_MNT_COST_AMT': 50.00, 'PLAN_DATA_LMT_GB': None, 'PLAN_VOICE_LMT_MIN': 0, 'PLAN_SMS_LMT_CNT': 0, 'PLAN_INTL_FLG': 'Y', 'PLAN_5G_FLG': 'Y', 'PLAN_STRT_DT': '2021-06-01', 'PLAN_END_DT': None, 'PLAN_STAT_CD': 'ACT'},
        {'PLAN_CD': 'COMBO_PREMIUM', 'PLAN_NM': 'Premium Combo', 'PLAN_TYP_CD': 'COMBO', 'PLAN_TIER_CD': 'PREMIUM', 'PLAN_MNT_COST_AMT': 80.00, 'PLAN_DATA_LMT_GB': 50.0, 'PLAN_VOICE_LMT_MIN': 2000, 'PLAN_SMS_LMT_CNT': 5000, 'PLAN_INTL_FLG': 'Y', 'PLAN_5G_FLG': 'Y', 'PLAN_STRT_DT': '2022-01-01', 'PLAN_END_DT': None, 'PLAN_STAT_CD': 'ACT'},
        {'PLAN_CD': 'IOT_STANDARD', 'PLAN_NM': 'IoT Standard', 'PLAN_TYP_CD': 'IOT', 'PLAN_TIER_CD': 'STANDARD', 'PLAN_MNT_COST_AMT': 5.00, 'PLAN_DATA_LMT_GB': 1.0, 'PLAN_VOICE_LMT_MIN': 0, 'PLAN_SMS_LMT_CNT': 50, 'PLAN_INTL_FLG': 'N', 'PLAN_5G_FLG': 'Y', 'PLAN_STRT_DT': '2023-01-01', 'PLAN_END_DT': None, 'PLAN_STAT_CD': 'ACT'},
    ]
    return pd.DataFrame(records)

def generate_subscribers(customers_df, num_records=8000):
    logger.info(f"Generating {num_records} SUBSCR_ACCT...")
    records = []
    cust_ids = customers_df['CUST_ID'].tolist()
    for i in range(1, num_records + 1):
        records.append({
            'SUBSCR_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'SUBSCR_IMSI_NO': str(fake.random_number(digits=15, fix_len=True)),
            'SUBSCR_MSISDN_NO': fake.phone_number()[:15],
            'SUBSCR_ICCID_NO': str(fake.random_number(digits=20, fix_len=True)),
            'SUBSCR_ACCT_TYP_CD': random.choice(['PREPAID', 'POSTPAID', 'HYBRID']),
            'SUBSCR_STAT_CD': random.choices(['ACT', 'SUS', 'TRM'], weights=[0.85, 0.1, 0.05])[0],
            'SUBSCR_ACTV_DT': fake.date_between(start_date='-3y', end_date='today').isoformat(),
            'SUBSCR_DEACT_DT': None,
            'SUBSCR_DATA_PLAN_CD': random.choice(['DATA_UNLMTD', 'COMBO_PREMIUM']),
            'SUBSCR_MNT_USG_GB': round(random.uniform(0, 100), 2),
            'SUBSCR_MNT_VOICE_MIN': random.randint(0, 1500),
            'SUBSCR_ROAM_FLG': random.choice(['Y', 'N']),
            'SUBSCR_CREAT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'SUBSCR_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)

# We will generate just enough data to populate the SQLite DB so the pipeline can ingest it
def main():
    engine = setup_database()
    
    customers_df = generate_customers(100) # Small sample for fast POC run
    plans_df = generate_service_plans()
    subscribers_df = generate_subscribers(customers_df, 150)
    
    logger.info("Writing data to SQLite...")
    customers_df.to_sql('CUST_MSTR', engine, if_exists='append', index=False)
    plans_df.to_sql('SVC_PLAN_REF', engine, if_exists='append', index=False)
    subscribers_df.to_sql('SUBSCR_ACCT', engine, if_exists='append', index=False)
    
    # We omit the other 19 tables' data generation logic for brevity in this POC,
    # as the schemas are already in the DB from the DDL and Schema Ingestion layer
    # will still read their structure. To train CTGAN fully, we would need to generate
    # mock data for all tables here.
    
    logger.info("Seed data generation complete.")

if __name__ == '__main__':
    main()
