"""
Seed Data Generator for POC
Uses Faker to generate realistic telecom seed data into a source SQLite database.
This simulates the real production databases we would connect to.
Generates data for all 22 tables across 3 domains.
"""

import os
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import pandas as pd
from sqlalchemy import create_engine, text

fake = Faker('en_US')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = Path('datasets/telecom_source.db')
DDL_DIR = Path('datasets/ddl')


def setup_database():
    """Create the SQLite database and run DDL scripts to create schemas."""
    if DB_PATH.exists():
        DB_PATH.unlink()

    engine = create_engine(f'sqlite:///{DB_PATH}')

    for sql_file in sorted(DDL_DIR.glob('*.sql')):
        logger.info(f"Executing DDL: {sql_file.name}")
        with open(sql_file, 'r') as f:
            sql_statements = f.read().split(';')
            with engine.begin() as conn:
                for statement in sql_statements:
                    stmt = statement.strip()
                    if stmt:
                        try:
                            conn.execute(text(stmt))
                        except Exception as e:
                            logger.warning(f"DDL statement skipped: {e}")

    return engine


# ========== CUSTOMER DOMAIN ==========

def generate_customers(num_records=5000):
    """Generate CUST_MSTR records."""
    logger.info(f"Generating {num_records} CUST_MSTR...")
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
            'CUST_STAT_RSN_CD': random.choice(['DEFAULT', 'NON_PAYMENT', 'REQUEST', 'FRAUD']),
            'CUST_LANG_PREF_CD': random.choices(['EN', 'ES', 'FR'], weights=[0.8, 0.15, 0.05])[0],
            'CUST_CREAT_DT': acq_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'CUST_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'CUST_UPDT_BY': 'SYSTEM'
        })
    return pd.DataFrame(records)


def generate_service_plans():
    """Generate SVC_PLAN_REF records."""
    logger.info("Generating SVC_PLAN_REF...")
    base_plans = [
        ('VOICE_BASIC', 'Basic Voice Plan', 'VOICE', 'BASIC', 20.00, 0, 500, 100, 'N', 'N'),
        ('DATA_UNLMTD', 'Unlimited Data', 'DATA', 'UNLIMITED', 50.00, None, 0, 0, 'Y', 'Y'),
        ('COMBO_PREMIUM', 'Premium Combo', 'COMBO', 'PREMIUM', 80.00, 50.0, 2000, 5000, 'Y', 'Y'),
        ('IOT_STANDARD', 'IoT Standard', 'IOT', 'STANDARD', 5.00, 1.0, 0, 50, 'N', 'Y'),
        ('FAMILY_SHARE', 'Family Share Plan', 'COMBO', 'FAMILY', 120.00, 100.0, 5000, 10000, 'Y', 'Y'),
        ('PREPAID_MIN', 'Prepaid Minimum', 'VOICE', 'BASIC', 10.00, 0.5, 100, 50, 'N', 'N'),
        ('DATA_5GB', 'Data 5GB Plan', 'DATA', 'BASIC', 25.00, 5.0, 0, 0, 'N', 'N'),
        ('DATA_25GB', 'Data 25GB Plan', 'DATA', 'STANDARD', 40.00, 25.0, 0, 0, 'N', 'Y'),
        ('COMBO_STANDARD', 'Standard Combo', 'COMBO', 'STANDARD', 55.00, 20.0, 1000, 2000, 'N', 'Y'),
        ('SENIOR_PLAN', 'Senior Value Plan', 'COMBO', 'BASIC', 30.00, 5.0, 500, 500, 'N', 'N'),
        ('STUDENT_PLAN', 'Student Discount Plan', 'COMBO', 'BASIC', 35.00, 15.0, 500, 1000, 'N', 'Y'),
        ('BUSI_BASIC', 'Business Basic', 'COMBO', 'STANDARD', 45.00, 10.0, 1000, 1000, 'Y', 'N'),
        ('BUSI_PREMIUM', 'Business Premium', 'COMBO', 'PREMIUM', 100.00, 75.0, 3000, 5000, 'Y', 'Y'),
        ('INTL_TRAVELER', 'International Traveler', 'COMBO', 'PREMIUM', 90.00, 30.0, 2000, 3000, 'Y', 'Y'),
        ('HOTSPOT_ONLY', 'Hotspot Only Plan', 'DATA', 'BASIC', 15.00, 10.0, 0, 0, 'N', 'N'),
    ]
    records = []
    for plan in base_plans:
        records.append({
            'PLAN_CD': plan[0],
            'PLAN_NM': plan[1],
            'PLAN_TYP_CD': plan[2],
            'PLAN_TIER_CD': plan[3],
            'PLAN_MNT_COST_AMT': plan[4],
            'PLAN_DATA_LMT_GB': plan[5],
            'PLAN_VOICE_LMT_MIN': plan[6],
            'PLAN_SMS_LMT_CNT': plan[7],
            'PLAN_INTL_FLG': plan[8],
            'PLAN_5G_FLG': plan[9],
            'PLAN_STRT_DT': fake.date_between(start_date='-5y', end_date='-1y').isoformat(),
            'PLAN_END_DT': None,
            'PLAN_STAT_CD': 'ACT'
        })
    # Add some expired plans
    for j in range(5):
        strt = fake.date_between(start_date='-8y', end_date='-4y')
        end = strt + timedelta(days=random.randint(365, 1095))
        records.append({
            'PLAN_CD': f'LEGACY_{j+1}',
            'PLAN_NM': f'Legacy Plan {j+1}',
            'PLAN_TYP_CD': random.choice(['VOICE', 'DATA', 'COMBO']),
            'PLAN_TIER_CD': random.choice(['BASIC', 'STANDARD']),
            'PLAN_MNT_COST_AMT': round(random.uniform(15, 60), 2),
            'PLAN_DATA_LMT_GB': round(random.uniform(0, 10), 1),
            'PLAN_VOICE_LMT_MIN': random.randint(100, 1000),
            'PLAN_SMS_LMT_CNT': random.randint(50, 500),
            'PLAN_INTL_FLG': 'N',
            'PLAN_5G_FLG': 'N',
            'PLAN_STRT_DT': strt.isoformat(),
            'PLAN_END_DT': end.isoformat(),
            'PLAN_STAT_CD': 'INACT'
        })
    return pd.DataFrame(records)


def generate_subscribers(cust_ids, num_records=8000):
    """Generate SUBSCR_ACCT records."""
    logger.info(f"Generating {num_records} SUBSCR_ACCT...")
    plan_codes = ['VOICE_BASIC', 'DATA_UNLMTD', 'COMBO_PREMIUM', 'IOT_STANDARD',
                  'FAMILY_SHARE', 'PREPAID_MIN', 'DATA_5GB', 'DATA_25GB',
                  'COMBO_STANDARD', 'SENIOR_PLAN', 'STUDENT_PLAN',
                  'BUSI_BASIC', 'BUSI_PREMIUM', 'INTL_TRAVELER', 'HOTSPOT_ONLY']
    records = []
    for i in range(1, num_records + 1):
        actv_dt = fake.date_between(start_date='-3y', end_date='today')
        stat = random.choices(['ACT', 'SUS', 'TRM'], weights=[0.85, 0.1, 0.05])[0]
        records.append({
            'SUBSCR_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'SUBSCR_IMSI_NO': str(fake.random_number(digits=15, fix_len=True)),
            'SUBSCR_MSISDN_NO': fake.numerify('+1##########'),
            'SUBSCR_ICCID_NO': str(fake.random_number(digits=20, fix_len=True)),
            'SUBSCR_ACCT_TYP_CD': random.choice(['PREPAID', 'POSTPAID', 'HYBRID']),
            'SUBSCR_STAT_CD': stat,
            'SUBSCR_ACTV_DT': actv_dt.isoformat(),
            'SUBSCR_DEACT_DT': (actv_dt + timedelta(days=random.randint(30, 730))).isoformat() if stat == 'TRM' else None,
            'SUBSCR_DATA_PLAN_CD': random.choice(plan_codes),
            'SUBSCR_MNT_USG_GB': round(random.uniform(0, 100), 2),
            'SUBSCR_MNT_VOICE_MIN': random.randint(0, 1500),
            'SUBSCR_ROAM_FLG': random.choices(['Y', 'N'], weights=[0.1, 0.9])[0],
            'SUBSCR_CREAT_DT': actv_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'SUBSCR_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_plan_assignments(subscr_ids, plan_codes, num_records=10000):
    """Generate SUBSCR_PLAN_ASSGN records."""
    logger.info(f"Generating {num_records} SUBSCR_PLAN_ASSGN...")
    records = []
    for i in range(1, num_records + 1):
        strt_dt = fake.date_between(start_date='-2y', end_date='today')
        has_end = random.random() < 0.3
        end_dt = (strt_dt + timedelta(days=random.randint(30, 365))).isoformat() if has_end else None
        stat = 'EXP' if has_end and random.random() < 0.7 else random.choices(['ACT', 'CXL'], weights=[0.85, 0.15])[0]
        base_chrg = round(random.uniform(10, 120), 2)
        disc_pct = round(random.choice([0, 0, 0, 5, 10, 15, 20, 25]), 2)
        records.append({
            'ASSGN_ID': i,
            'SUBSCR_ID': random.choice(subscr_ids),
            'PLAN_CD': random.choice(plan_codes),
            'ASSGN_STRT_DT': strt_dt.isoformat(),
            'ASSGN_END_DT': end_dt,
            'ASSGN_STAT_CD': stat,
            'ASSGN_MNT_CHRG_AMT': base_chrg,
            'ASSGN_DISC_PCT': disc_pct,
            'ASSGN_PROM_CD': random.choice([None, None, 'PROMO10', 'PROMO20', 'LOYALTY15', 'WELCOME25', 'BOGO50']),
            'ASSGN_AUTO_RNW_FLG': random.choices(['Y', 'N'], weights=[0.7, 0.3])[0],
            'ASSGN_CREAT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_customer_addresses(cust_ids, num_records=6000):
    """Generate CUST_ADDR records."""
    logger.info(f"Generating {num_records} CUST_ADDR...")
    records = []
    for i in range(1, num_records + 1):
        creat_dt = fake.date_between(start_date='-3y', end_date='today')
        records.append({
            'ADDR_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'ADDR_TYP_CD': random.choice(['HOME', 'WORK', 'BILLING', 'SHIPPING']),
            'ADDR_LN_1': fake.street_address(),
            'ADDR_LN_2': fake.secondary_address() if random.random() < 0.3 else None,
            'ADDR_CITY_NM': fake.city(),
            'ADDR_ST_CD': fake.state_abbr(),
            'ADDR_ZIP_CD': fake.zipcode(),
            'ADDR_CNTRY_CD': 'US',
            'ADDR_PRI_FLG': 'Y' if random.random() < 0.5 else 'N',
            'ADDR_VALID_FLG': random.choices(['Y', 'N'], weights=[0.95, 0.05])[0],
            'ADDR_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ADDR_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_customer_contacts(cust_ids, num_records=7000):
    """Generate CUST_CNTCT records."""
    logger.info(f"Generating {num_records} CUST_CNTCT...")
    records = []
    for i in range(1, num_records + 1):
        creat_dt = fake.date_between(start_date='-3y', end_date='today')
        cntct_typ = random.choice(['PHONE', 'EMAIL', 'SMS', 'MAIL'])
        if cntct_typ in ('PHONE', 'SMS'):
            val = fake.numerify('+1##########')
        elif cntct_typ == 'EMAIL':
            val = fake.email()
        else:
            val = fake.address().replace('\n', ', ')
        vrfy = random.choices(['Y', 'N'], weights=[0.8, 0.2])[0]
        records.append({
            'CNTCT_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'CNTCT_TYP_CD': cntct_typ,
            'CNTCT_VAL': val,
            'CNTCT_PRI_FLG': 'Y' if random.random() < 0.4 else 'N',
            'CNTCT_OPT_IN_FLG': random.choice(['Y', 'N']),
            'CNTCT_VRFY_FLG': vrfy,
            'CNTCT_VRFY_DT': creat_dt.isoformat() if vrfy == 'Y' else None,
            'CNTCT_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'CNTCT_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_identity_documents(cust_ids, num_records=5000):
    """Generate IDENT_DOC records."""
    logger.info(f"Generating {num_records} IDENT_DOC...")
    records = []
    for i in range(1, num_records + 1):
        doc_type = random.choice(['SSN', 'PASSPORT', 'DRIVERS_LICENSE', 'STATE_ID'])
        iss_dt = fake.date_between(start_date='-10y', end_date='-1y')
        records.append({
            'DOC_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'DOC_TYP_CD': doc_type,
            'DOC_NO': fake.ssn().replace('-', '') if doc_type == 'SSN' else fake.bothify('???######'),
            'DOC_ISS_DT': iss_dt.isoformat(),
            'DOC_EXP_DT': fake.date_between(start_date='today', end_date='+5y').isoformat(),
            'DOC_ISS_AUTH_NM': random.choice([
                'State of California', 'State of Texas', 'State of New York',
                'State of Florida', 'US Department of State', 'State of Illinois',
                'State of Ohio', 'State of Georgia', 'State of Pennsylvania'
            ]),
            'DOC_VRFY_STAT_CD': random.choices(['VERIFIED', 'PENDING', 'FAILED'], weights=[0.8, 0.15, 0.05])[0],
            'DOC_CREAT_DT': fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_customer_status_history(cust_ids, num_records=8000):
    """Generate CUST_STAT_HIST records."""
    logger.info(f"Generating {num_records} CUST_STAT_HIST...")
    statuses = ['ACT', 'SUS', 'TRM', 'PDG']
    records = []
    for i in range(1, num_records + 1):
        chg_dt = fake.date_time_between(start_date='-3y', end_date='now')
        prev = random.choice(statuses)
        new = random.choice([s for s in statuses if s != prev])
        records.append({
            'HIST_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'PREV_STAT_CD': prev,
            'NEW_STAT_CD': new,
            'STAT_CHG_RSN_CD': random.choice(['PAYMENT', 'REQUEST', 'SYSTEM', 'FRAUD', 'MIGRATION']),
            'STAT_CHG_DT': chg_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'STAT_CHG_BY': random.choice(['SYSTEM', 'AGENT', 'CUSTOMER']),
            'STAT_CHG_CHNL_CD': random.choice(['WEB', 'STORE', 'CALL_CENTER', 'APP', 'SYSTEM']),
            'STAT_CHG_NOTES': fake.sentence() if random.random() < 0.3 else None
        })
    return pd.DataFrame(records)


# ========== BILLING DOMAIN ==========

def generate_billing_accounts(cust_ids, num_records=5000):
    """Generate BLNG_ACCT records."""
    logger.info(f"Generating {num_records} BLNG_ACCT...")
    records = []
    for i in range(1, num_records + 1):
        creat_dt = fake.date_between(start_date='-4y', end_date='-6m')
        curr_bal = round(random.uniform(-50, 500), 2)
        past_due = round(random.uniform(0, 200), 2) if random.random() < 0.15 else 0.0
        records.append({
            'BLNG_ACCT_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'BLNG_ACCT_NO': f'BA-{fake.random_number(digits=10, fix_len=True)}',
            'BLNG_TYP_CD': random.choice(['INDIVIDUAL', 'FAMILY', 'BUSINESS']),
            'BLNG_CYC_CD': random.choice(['CYC01', 'CYC15', 'CYC28']),
            'BLNG_PYMT_TERM_CD': random.choice(['NET30', 'NET15', 'DUE_ON_RCPT', 'NET45']),
            'BLNG_CRED_LMT_AMT': round(random.choice([500, 1000, 2000, 5000]), 2),
            'BLNG_CURR_BAL_AMT': curr_bal,
            'BLNG_PAST_DUE_AMT': past_due,
            'BLNG_AUTOPAY_FLG': random.choices(['Y', 'N'], weights=[0.6, 0.4])[0],
            'BLNG_PPRLSS_FLG': random.choices(['Y', 'N'], weights=[0.7, 0.3])[0],
            'BLNG_STAT_CD': random.choices(['ACT', 'SUS', 'CLS'], weights=[0.85, 0.1, 0.05])[0],
            'BLNG_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'BLNG_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_invoices(blng_acct_ids, num_records=15000):
    """Generate INVC records."""
    logger.info(f"Generating {num_records} INVC...")
    records = []
    for i in range(1, num_records + 1):
        cyc_dt = fake.date_between(start_date='-2y', end_date='today')
        due_dt = cyc_dt + timedelta(days=30)
        subtot = round(random.uniform(20, 250), 2)
        tax = round(subtot * random.uniform(0.05, 0.12), 2)
        disc = round(random.uniform(0, 20), 2) if random.random() < 0.2 else 0.0
        tot = round(subtot + tax - disc, 2)
        stat = random.choices(['PAID', 'OPEN', 'PAST_DUE', 'VOID'], weights=[0.6, 0.2, 0.15, 0.05])[0]
        paid_amt = tot if stat == 'PAID' else (round(random.uniform(0, tot), 2) if stat == 'PAST_DUE' else 0.0)
        bal = round(tot - paid_amt, 2)
        iss_dt = cyc_dt
        paid_dt = (cyc_dt + timedelta(days=random.randint(1, 28))).isoformat() if stat == 'PAID' else None
        records.append({
            'INVC_ID': i,
            'BLNG_ACCT_ID': random.choice(blng_acct_ids),
            'INVC_NO': f'INV-{fake.random_number(digits=10, fix_len=True)}',
            'INVC_CYC_DT': cyc_dt.isoformat(),
            'INVC_DUE_DT': due_dt.isoformat(),
            'INVC_SUBTOT_AMT': subtot,
            'INVC_TAX_AMT': tax,
            'INVC_DISC_AMT': disc,
            'INVC_TOT_AMT': tot,
            'INVC_PAID_AMT': paid_amt,
            'INVC_BAL_AMT': bal,
            'INVC_STAT_CD': stat,
            'INVC_ISS_DT': iss_dt.isoformat(),
            'INVC_PAID_DT': paid_dt,
            'INVC_CREAT_DT': cyc_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_invoice_line_items(invc_ids, num_records=40000):
    """Generate INVC_LN_ITEM records."""
    logger.info(f"Generating {num_records} INVC_LN_ITEM...")
    records = []
    for i in range(1, num_records + 1):
        svc_strt = fake.date_between(start_date='-60d', end_date='today')
        svc_end = svc_strt + timedelta(days=30)
        qty = random.randint(1, 10)
        unit_prc = round(random.uniform(1, 50), 2)
        disc_pct = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)
        disc_amt = round(qty * unit_prc * disc_pct / 100, 2)
        subtotal = round(qty * unit_prc - disc_amt, 2)
        tax_amt = round(subtotal * random.uniform(0.05, 0.1), 2)
        tot_amt = round(subtotal + tax_amt, 2)
        records.append({
            'LN_ITEM_ID': i,
            'INVC_ID': random.choice(invc_ids),
            'LN_ITEM_SEQ_NO': random.randint(1, 20),
            'LN_ITEM_TYP_CD': random.choice(['RECURRING', 'USAGE', 'ONE_TIME', 'TAX', 'CREDIT', 'SURCHARGE']),
            'LN_ITEM_DESC': random.choice([
                'Monthly Plan Charge', 'Data Overage', 'Roaming Fee',
                'Device Payment', 'Insurance', 'Activation Fee',
                'Late Payment Fee', 'International Call', 'SMS Bundle',
                'Premium Content', 'Equipment Lease', 'Regulatory Fee'
            ]),
            'LN_ITEM_SVC_STRT_DT': svc_strt.isoformat(),
            'LN_ITEM_SVC_END_DT': svc_end.isoformat(),
            'LN_ITEM_QTY': qty,
            'LN_ITEM_UNIT_PRC_AMT': unit_prc,
            'LN_ITEM_DISC_PCT': disc_pct,
            'LN_ITEM_DISC_AMT': disc_amt,
            'LN_ITEM_TAX_AMT': tax_amt,
            'LN_ITEM_TOT_AMT': tot_amt,
            'LN_ITEM_GL_ACCT_CD': random.choice(['4100', '4200', '4300', '4400', '4500', '4600']),
            'LN_ITEM_CREAT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_payments(blng_acct_ids, pymt_mthd_ids, invc_ids, num_records=12000):
    """Generate PYMT records."""
    logger.info(f"Generating {num_records} PYMT...")
    records = []
    for i in range(1, num_records + 1):
        pymt_dt = fake.date_between(start_date='-2y', end_date='today')
        if random.random() < 0.75:
            amt = round(random.uniform(50, 200), 2)
        else:
            amt = round(random.uniform(10, 50), 2)
        stat = random.choices(['COMPLETED', 'PENDING', 'FAILED', 'REVERSED'], weights=[0.85, 0.05, 0.05, 0.05])[0]
        records.append({
            'PYMT_ID': i,
            'BLNG_ACCT_ID': random.choice(blng_acct_ids),
            'PYMT_MTHD_ID': random.choice(pymt_mthd_ids),
            'INVC_ID': random.choice(invc_ids),
            'PYMT_AMT': amt,
            'PYMT_DT': pymt_dt.isoformat(),
            'PYMT_TYP_CD': random.choice(['REGULAR', 'ADVANCE', 'PARTIAL', 'OVERPAYMENT']),
            'PYMT_STAT_CD': stat,
            'PYMT_CHNL_CD': random.choice(['ONLINE', 'STORE', 'PHONE', 'AUTOPAY', 'MAIL']),
            'PYMT_CONF_NO': f'PY-{fake.random_number(digits=12, fix_len=True)}',
            'PYMT_FAIL_RSN_CD': random.choice(['INSUF_FUNDS', 'EXPIRED_CARD', 'DECLINED', 'TECH_ERR']) if stat == 'FAILED' else None,
            'PYMT_PROC_DT': (pymt_dt + timedelta(days=random.randint(0, 3))).isoformat(),
            'PYMT_CREAT_DT': pymt_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_payment_methods(cust_ids, num_records=6000):
    """Generate PYMT_MTHD records."""
    logger.info(f"Generating {num_records} PYMT_MTHD...")
    records = []
    for i in range(1, num_records + 1):
        mthd_type = random.choice(['CREDIT_CARD', 'DEBIT_CARD', 'ACH', 'DIGITAL_WALLET'])
        creat_dt = fake.date_between(start_date='-3y', end_date='today')
        is_card = mthd_type in ('CREDIT_CARD', 'DEBIT_CARD')
        is_ach = mthd_type == 'ACH'
        is_wallet = mthd_type == 'DIGITAL_WALLET'
        records.append({
            'PYMT_MTHD_ID': i,
            'CUST_ID': random.choice(cust_ids),
            'MTHD_TYP_CD': mthd_type,
            'MTHD_CARD_NO': f'****{random.randint(1000, 9999)}' if is_card else None,
            'MTHD_CARD_BRAND_CD': random.choice(['VISA', 'MC', 'AMEX', 'DISC']) if is_card else None,
            'MTHD_CARD_EXP_MM': random.randint(1, 12) if is_card else None,
            'MTHD_CARD_EXP_YY': random.randint(2025, 2030) if is_card else None,
            'MTHD_BNK_ROUT_NO': fake.numerify('#########') if is_ach else None,
            'MTHD_BNK_ACCT_NO': fake.numerify('######' + '#' * random.randint(0, 6)) if is_ach else None,
            'MTHD_BNK_NM': fake.company() + ' Bank' if is_ach else None,
            'MTHD_DGTL_WLLT_ID': fake.uuid4()[:16] if is_wallet else None,
            'MTHD_NICKM': random.choice(['My Visa', 'Personal Card', 'Work Card', 'Checking', 'Savings', 'Apple Pay', None]),
            'MTHD_PRI_FLG': 'Y' if random.random() < 0.4 else 'N',
            'MTHD_STAT_CD': random.choices(['ACT', 'EXP', 'RVK'], weights=[0.8, 0.15, 0.05])[0],
            'MTHD_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'MTHD_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_usage_records(subscr_ids, num_records=25000):
    """Generate USAGE_REC records."""
    logger.info(f"Generating {num_records} USAGE_REC...")
    records = []
    for i in range(1, num_records + 1):
        strt_dt = fake.date_time_between(start_date='-90d', end_date='now')
        dur = random.randint(10, 3600) if random.random() < 0.5 else 0
        end_dt = strt_dt + timedelta(seconds=dur)
        usg_typ = random.choice(['VOICE', 'DATA', 'SMS', 'MMS', 'ROAM_VOICE', 'ROAM_DATA'])
        is_roam = usg_typ.startswith('ROAM')
        is_intl = random.random() < 0.08
        data_kb = round(random.uniform(0, 512000), 2) if 'DATA' in usg_typ else 0
        records.append({
            'USAGE_ID': i,
            'SUBSCR_ID': random.choice(subscr_ids),
            'USAGE_TYP_CD': usg_typ,
            'USAGE_STRT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'USAGE_END_DT': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'USAGE_DUR_SEC': dur,
            'USAGE_DATA_KB': data_kb,
            'USAGE_ORIG_NO': fake.numerify('+1##########'),
            'USAGE_DEST_NO': fake.numerify('+1##########'),
            'USAGE_CELL_ID': f'CELL-{random.randint(10000, 99999)}',
            'USAGE_ROAM_FLG': 'Y' if is_roam else 'N',
            'USAGE_INTL_FLG': 'Y' if is_intl else 'N',
            'USAGE_CHRG_AMT': round(random.uniform(0, 25), 2),
            'USAGE_RATE_PLAN_CD': random.choice(['VOICE_BASIC', 'DATA_UNLMTD', 'COMBO_PREMIUM',
                                                  'PREPAID_MIN', 'COMBO_STANDARD']),
            'USAGE_CREAT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_cdr_records(subscr_ids, num_records=30000):
    """Generate CDR_REC records."""
    logger.info(f"Generating {num_records} CDR_REC...")
    records = []
    for i in range(1, num_records + 1):
        strt_dt = fake.date_time_between(start_date='-90d', end_date='now')
        dur = random.randint(5, 3600)
        end_dt = strt_dt + timedelta(seconds=dur)
        fraud_scr = round(random.uniform(0, 1), 4)
        rated_dur = dur if random.random() < 0.9 else random.randint(dur, dur + 120)
        records.append({
            'CDR_ID': i,
            'SUBSCR_ID': random.choice(subscr_ids),
            'CDR_SEQ_NO': random.randint(1, 999999),
            'CDR_TYP_CD': random.choice(['MO_VOICE', 'MT_VOICE', 'MO_SMS', 'MT_SMS', 'MO_DATA', 'VoLTE']),
            'CDR_ORIG_MSISDN': fake.numerify('+1##########'),
            'CDR_DEST_MSISDN': fake.numerify('+1##########'),
            'CDR_ORIG_IMSI': str(fake.random_number(digits=15, fix_len=True)),
            'CDR_STRT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'CDR_END_DT': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'CDR_DUR_SEC': dur,
            'CDR_ORIG_CELL_ID': f'CELL-{random.randint(10000, 99999)}',
            'CDR_DEST_CELL_ID': f'CELL-{random.randint(10000, 99999)}',
            'CDR_CALL_RSLT_CD': random.choices(['SUCCESS', 'FAILED', 'DROPPED', 'BUSY', 'NO_ANSWER'],
                                                weights=[0.85, 0.05, 0.03, 0.04, 0.03])[0],
            'CDR_ROAM_IND_CD': random.choices(['HOME', 'NATIONAL', 'INTERNATIONAL'],
                                               weights=[0.88, 0.08, 0.04])[0],
            'CDR_CHRG_AMT': round(random.uniform(0, 10), 2),
            'CDR_RATED_DUR_SEC': rated_dur,
            'CDR_FRAUD_SCR': fraud_scr,
            'CDR_FRAUD_FLG': 'Y' if fraud_scr > 0.85 else 'N',
            'CDR_CREAT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


# ========== NETWORK DOMAIN ==========

def generate_network_elements(num_records=500):
    """Generate NTWK_ELEM records."""
    logger.info(f"Generating {num_records} NTWK_ELEM...")
    regions = ['NORTHEAST', 'SOUTHEAST', 'MIDWEST', 'WEST', 'SOUTHWEST', 'PACIFIC', 'MOUNTAIN']
    records = []
    for i in range(1, num_records + 1):
        install_dt = fake.date_between(start_date='-8y', end_date='-1y')
        last_maint = fake.date_between(start_date=install_dt, end_date='today')
        max_conn = random.choice([500, 1000, 2000, 5000, 10000])
        curr_conn = random.randint(0, max_conn)
        records.append({
            'ELEM_ID': i,
            'ELEM_NM': f'{random.choice(["RTR", "SWT", "BTS", "eNB", "gNB"])}-{fake.bothify("??###")}',
            'ELEM_TYP_CD': random.choice(['ROUTER', 'SWITCH', 'BTS', 'eNodeB', 'gNodeB', 'CORE']),
            'ELEM_VNDR_NM': random.choice(['Ericsson', 'Nokia', 'Huawei', 'Samsung', 'Cisco']),
            'ELEM_MDL_NO': fake.bothify('??-####'),
            'ELEM_SRIAL_NO': fake.bothify('SN-??########'),
            'ELEM_LAT_COORD': round(random.uniform(25, 48), 6),
            'ELEM_LON_COORD': round(random.uniform(-125, -67), 6),
            'ELEM_SITE_CD': f'SITE-{random.randint(100, 999)}',
            'ELEM_RGN_CD': random.choice(regions),
            'ELEM_CAP_PCT': round(curr_conn / max_conn * 100, 2) if max_conn > 0 else 0,
            'ELEM_MAX_CONN_CNT': max_conn,
            'ELEM_CURR_CONN_CNT': curr_conn,
            'ELEM_FRWRE_VER': f'{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}',
            'ELEM_INSTL_DT': install_dt.isoformat(),
            'ELEM_LST_MAINT_DT': last_maint.isoformat(),
            'ELEM_STAT_CD': random.choices(['ACTIVE', 'MAINT', 'DECOMM'], weights=[0.85, 0.1, 0.05])[0],
            'ELEM_CREAT_DT': install_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ELEM_UPDT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_cell_towers(elem_ids, num_records=300):
    """Generate CELL_TWR records."""
    logger.info(f"Generating {num_records} CELL_TWR...")
    records = []
    for i in range(1, num_records + 1):
        creat_dt = fake.date_between(start_date='-6y', end_date='-6m')
        lease_exp = fake.date_between(start_date='today', end_date='+10y')
        records.append({
            'TWR_ID': i,
            'ELEM_ID': random.choice(elem_ids),
            'TWR_NM': f'TWR-{fake.bothify("???###")}',
            'TWR_TYP_CD': random.choice(['MACRO', 'MICRO', 'SMALL', 'FEMTO']),
            'TWR_HGT_FT': random.randint(50, 300),
            'TWR_FREQ_BAND_CD': random.choice(['700MHz', '850MHz', '1900MHz', '2100MHz', '3500MHz', 'mmWave']),
            'TWR_TECH_CD': random.choice(['4G_LTE', '5G_NR', '3G_UMTS', '5G_mmW']),
            'TWR_CVG_RAD_MI': round(random.uniform(0.5, 25), 2),
            'TWR_SECT_CNT': random.choice([1, 3, 6]),
            'TWR_PWR_SRC_CD': random.choice(['GRID', 'SOLAR', 'GENERATOR', 'HYBRID']),
            'TWR_BKUP_PWR_HRS': round(random.uniform(4, 72), 1),
            'TWR_OWNR_TYP_CD': random.choice(['OWNED', 'LEASED', 'SHARED', 'COLOC']),
            'TWR_LEASE_EXP_DT': lease_exp.isoformat(),
            'TWR_STAT_CD': random.choices(['ACTIVE', 'MAINT', 'PLANNED'], weights=[0.85, 0.1, 0.05])[0],
            'TWR_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_service_orders(subscr_ids, num_records=10000):
    """Generate SVC_ORD records. References SUBSCR_ACCT.SUBSCR_ID."""
    logger.info(f"Generating {num_records} SVC_ORD...")
    records = []
    for i in range(1, num_records + 1):
        creat_dt = fake.date_between(start_date='-2y', end_date='today')
        submit_dt = creat_dt + timedelta(days=random.randint(0, 1))
        prov_dt = submit_dt + timedelta(days=random.randint(0, 3))
        actv_dt = prov_dt + timedelta(days=random.randint(0, 5))
        comp_dt = actv_dt + timedelta(days=random.randint(0, 7))
        stat = random.choices(['COMP', 'PROV', 'ACTV', 'PDG', 'CXL'], weights=[0.5, 0.15, 0.15, 0.15, 0.05])[0]
        is_cxl = stat == 'CXL'
        subtot = round(random.uniform(0, 500), 2)
        disc = round(random.uniform(0, subtot * 0.2), 2) if random.random() < 0.3 else 0.0
        tax = round((subtot - disc) * random.uniform(0.05, 0.1), 2)
        tot = round(subtot - disc + tax, 2)
        records.append({
            'ORD_ID': i,
            'SUBSCR_ID': random.choice(subscr_ids),
            'ORD_TYP_CD': random.choice(['NEW', 'UPGRADE', 'DOWNGRADE', 'CANCEL', 'TRANSFER', 'REPAIR']),
            'ORD_STAT_CD': stat,
            'ORD_PRI_CD': random.choice(['HIGH', 'MEDIUM', 'LOW']),
            'ORD_CREAT_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ORD_SUBMIT_DT': submit_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ORD_PROV_DT': prov_dt.strftime('%Y-%m-%d %H:%M:%S') if stat not in ('PDG', 'CXL') else None,
            'ORD_ACTV_DT': actv_dt.strftime('%Y-%m-%d %H:%M:%S') if stat in ('ACTV', 'COMP') else None,
            'ORD_COMP_DT': comp_dt.strftime('%Y-%m-%d %H:%M:%S') if stat == 'COMP' else None,
            'ORD_CXL_DT': creat_dt.strftime('%Y-%m-%d %H:%M:%S') if is_cxl else None,
            'ORD_CXL_RSN_CD': random.choice(['CUST_REQ', 'DUPLICATE', 'ERROR', 'FRAUD']) if is_cxl else None,
            'ORD_SRC_CHNL_CD': random.choice(['WEB', 'STORE', 'CALL_CENTER', 'APP']),
            'ORD_AGT_ID': random.randint(1, 200) if random.random() < 0.7 else None,
            'ORD_TOT_AMT': tot,
            'ORD_DISC_AMT': disc,
            'ORD_TAX_AMT': tax,
            'ORD_NOTES': fake.sentence() if random.random() < 0.2 else None
        })
    return pd.DataFrame(records)


def generate_service_order_items(ord_ids, plan_codes, num_records=20000):
    """Generate SVC_ORD_ITEM records."""
    logger.info(f"Generating {num_records} SVC_ORD_ITEM...")
    records = []
    for i in range(1, num_records + 1):
        qty = random.randint(1, 3)
        unit_prc = round(random.uniform(0, 200), 2)
        tot = round(qty * unit_prc, 2)
        disc = round(random.uniform(0, tot * 0.15), 2) if random.random() < 0.2 else 0.0
        tax = round((tot - disc) * random.uniform(0.05, 0.1), 2)
        records.append({
            'ITEM_ID': i,
            'ORD_ID': random.choice(ord_ids),
            'PLAN_CD': random.choice(plan_codes),
            'ITEM_TYP_CD': random.choice(['PLAN_CHANGE', 'DEVICE', 'ADDON', 'SIM_SWAP', 'PORT_IN', 'PORT_OUT']),
            'ITEM_DESC': random.choice([
                'Plan upgrade to Premium', 'New device activation',
                'International add-on', 'SIM card replacement',
                'Number port-in', 'Insurance add-on',
                'Hotspot add-on', '5G upgrade', 'Device protection'
            ]),
            'ITEM_QTY': qty,
            'ITEM_UNIT_PRC_AMT': unit_prc,
            'ITEM_TOT_AMT': tot,
            'ITEM_DISC_AMT': disc,
            'ITEM_TAX_AMT': tax,
            'ITEM_STAT_CD': random.choices(['COMP', 'PDG', 'CXL'], weights=[0.7, 0.2, 0.1])[0],
            'ITEM_CREAT_DT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_work_order_assignments(ord_ids, agt_ids, num_records=8000):
    """Generate WRK_ORD_ASSGN records."""
    logger.info(f"Generating {num_records} WRK_ORD_ASSGN...")
    records = []
    for i in range(1, num_records + 1):
        schd_dt = fake.date_time_between(start_date='-1y', end_date='now')
        strt_dt = schd_dt + timedelta(hours=random.randint(0, 4))
        dur_min = random.randint(15, 480)
        comp_dt = strt_dt + timedelta(minutes=dur_min)
        stat = random.choices(['ASSIGNED', 'IN_PROGRESS', 'COMPLETED', 'ESCALATED'],
                               weights=[0.2, 0.3, 0.4, 0.1])[0]
        records.append({
            'ASSGN_ID': i,
            'ORD_ID': random.choice(ord_ids),
            'AGT_ID': random.choice(agt_ids),
            'ASSGN_TYP_CD': random.choice(['INSTALL', 'REPAIR', 'MAINTENANCE', 'UPGRADE', 'SURVEY']),
            'ASSGN_STAT_CD': stat,
            'ASSGN_PRI_CD': random.choice(['HIGH', 'MEDIUM', 'LOW']),
            'ASSGN_SCHD_DT': schd_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ASSGN_STRT_DT': strt_dt.strftime('%Y-%m-%d %H:%M:%S') if stat != 'ASSIGNED' else None,
            'ASSGN_COMP_DT': comp_dt.strftime('%Y-%m-%d %H:%M:%S') if stat == 'COMPLETED' else None,
            'ASSGN_DUR_MIN': dur_min if stat == 'COMPLETED' else None,
            'ASSGN_TRAVEL_MI': round(random.uniform(1, 80), 1) if stat != 'ASSIGNED' else None,
            'ASSGN_NOTES': fake.sentence() if random.random() < 0.3 else None,
            'ASSGN_CUST_RTNG': random.randint(1, 5) if stat == 'COMPLETED' and random.random() < 0.6 else None,
            'ASSGN_CREAT_DT': schd_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_network_incidents(elem_ids, num_records=3000):
    """Generate NTWK_INCDT records."""
    logger.info(f"Generating {num_records} NTWK_INCDT...")
    records = []
    for i in range(1, num_records + 1):
        rpt_dt = fake.date_time_between(start_date='-1y', end_date='now')
        ttr_min = random.randint(5, 2880)
        ack_dt = rpt_dt + timedelta(minutes=random.randint(1, 60))
        rsl_dt = rpt_dt + timedelta(minutes=ttr_min)
        cls_dt = rsl_dt + timedelta(minutes=random.randint(10, 120))
        stat = random.choices(['OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED'], weights=[0.1, 0.15, 0.25, 0.5])[0]
        records.append({
            'INCDT_ID': i,
            'ELEM_ID': random.choice(elem_ids),
            'INCDT_TYP_CD': random.choice(['OUTAGE', 'DEGRADATION', 'HARDWARE_FAIL', 'SOFTWARE_BUG', 'CAPACITY']),
            'INCDT_SVRTY_CD': random.choices(['P1', 'P2', 'P3', 'P4'], weights=[0.05, 0.15, 0.4, 0.4])[0],
            'INCDT_STAT_CD': stat,
            'INCDT_IMPCT_LVL_CD': random.choice(['CRITICAL', 'MAJOR', 'MINOR', 'INFORMATIONAL']),
            'INCDT_CUST_IMPCT_CNT': random.randint(0, 50000),
            'INCDT_RPT_DT': rpt_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'INCDT_ACK_DT': ack_dt.strftime('%Y-%m-%d %H:%M:%S') if stat != 'OPEN' else None,
            'INCDT_RSL_DT': rsl_dt.strftime('%Y-%m-%d %H:%M:%S') if stat in ('RESOLVED', 'CLOSED') else None,
            'INCDT_CLS_DT': cls_dt.strftime('%Y-%m-%d %H:%M:%S') if stat == 'CLOSED' else None,
            'INCDT_TTR_MIN': ttr_min if stat in ('RESOLVED', 'CLOSED') else None,
            'INCDT_ROOT_CAUSE_CD': random.choice([
                'POWER_FAILURE', 'FIBER_CUT', 'SW_CRASH', 'HW_DEFECT',
                'CAPACITY_LIMIT', 'CONFIG_ERROR', 'WEATHER', 'UNKNOWN'
            ]) if stat in ('RESOLVED', 'CLOSED') else None,
            'INCDT_DESC': random.choice([
                'Network element unresponsive', 'High packet loss detected',
                'Intermittent connectivity issues', 'Capacity threshold exceeded',
                'Hardware component failure', 'Software crash on restart',
                'Power supply unit malfunction', 'Fiber optic cable damage',
                'Configuration drift detected', 'Temperature alarm triggered'
            ]),
            'INCDT_RSL_NOTES': fake.sentence() if stat in ('RESOLVED', 'CLOSED') else None,
            'INCDT_CREAT_DT': rpt_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def generate_field_agents(num_records=200):
    """Generate FIELD_AGT records."""
    logger.info(f"Generating {num_records} FIELD_AGT...")
    records = []
    for i in range(1, num_records + 1):
        hire_dt = fake.date_between(start_date='-10y', end_date='-6m')
        records.append({
            'AGT_ID': i,
            'AGT_FRST_NM': fake.first_name(),
            'AGT_LST_NM': fake.last_name(),
            'AGT_EMP_ID': f'EMP-{fake.random_number(digits=6, fix_len=True)}',
            'AGT_EMAIL': fake.company_email(),
            'AGT_PHN_NO': fake.numerify('+1##########'),
            'AGT_CERT_LST': random.choice([
                'FIBER,WIRELESS', 'CORE_NETWORK', 'FIBER', 'WIRELESS,5G',
                'FIBER,CORE_NETWORK,WIRELESS', '5G,mmWave', 'GENERAL',
                'WIRELESS', 'FIBER,5G', None
            ]),
            'AGT_SKILL_LVL_CD': random.choice(['JUNIOR', 'MID', 'SENIOR', 'LEAD', 'EXPERT']),
            'AGT_RGN_CD': random.choice(['NORTHEAST', 'SOUTHEAST', 'MIDWEST', 'WEST', 'SOUTHWEST']),
            'AGT_AVAIL_STAT_CD': random.choices(['AVAILABLE', 'BUSY', 'OFF_DUTY', 'ON_LEAVE'],
                                                 weights=[0.5, 0.25, 0.15, 0.1])[0],
            'AGT_HIRE_DT': hire_dt.isoformat(),
            'AGT_STAT_CD': random.choices(['ACTIVE', 'ON_LEAVE', 'TERMINATED'], weights=[0.85, 0.1, 0.05])[0],
            'AGT_CREAT_DT': hire_dt.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(records)


def main():
    engine = setup_database()

    # ===== Customer Domain =====
    customers_df = generate_customers(5000)
    plans_df = generate_service_plans()
    cust_ids = customers_df['CUST_ID'].tolist()
    plan_codes = plans_df['PLAN_CD'].tolist()

    subscribers_df = generate_subscribers(cust_ids, 8000)
    subscr_ids = subscribers_df['SUBSCR_ID'].tolist()

    plan_assgn_df = generate_plan_assignments(subscr_ids, plan_codes, 10000)
    addresses_df = generate_customer_addresses(cust_ids, 6000)
    contacts_df = generate_customer_contacts(cust_ids, 7000)
    ident_docs_df = generate_identity_documents(cust_ids, 5000)
    status_hist_df = generate_customer_status_history(cust_ids, 8000)

    # ===== Billing Domain =====
    billing_accts_df = generate_billing_accounts(cust_ids, 5000)
    blng_acct_ids = billing_accts_df['BLNG_ACCT_ID'].tolist()

    pymt_methods_df = generate_payment_methods(cust_ids, 6000)
    pymt_mthd_ids = pymt_methods_df['PYMT_MTHD_ID'].tolist()

    invoices_df = generate_invoices(blng_acct_ids, 15000)
    invc_ids = invoices_df['INVC_ID'].tolist()

    line_items_df = generate_invoice_line_items(invc_ids, 40000)
    payments_df = generate_payments(blng_acct_ids, pymt_mthd_ids, invc_ids, 12000)
    usage_df = generate_usage_records(subscr_ids, 25000)
    cdr_df = generate_cdr_records(subscr_ids, 30000)

    # ===== Network Domain =====
    # FIELD_AGT must be generated BEFORE WRK_ORD_ASSGN (FK dependency)
    field_agents_df = generate_field_agents(200)
    agt_ids = field_agents_df['AGT_ID'].tolist()

    # NTWK_ELEM must be generated BEFORE CELL_TWR (FK dependency)
    ntwk_elems_df = generate_network_elements(500)
    elem_ids = ntwk_elems_df['ELEM_ID'].tolist()

    cell_towers_df = generate_cell_towers(elem_ids, 300)

    # SVC_ORD references SUBSCR_ACCT.SUBSCR_ID
    svc_orders_df = generate_service_orders(subscr_ids, 10000)
    ord_ids = svc_orders_df['ORD_ID'].tolist()

    svc_ord_items_df = generate_service_order_items(ord_ids, plan_codes, 20000)
    wrk_ord_assgn_df = generate_work_order_assignments(ord_ids, agt_ids, 8000)
    ntwk_incidents_df = generate_network_incidents(elem_ids, 3000)

    # ===== Write all tables =====
    logger.info("Writing all data to SQLite...")
    table_map = {
        'CUST_MSTR': customers_df,
        'SVC_PLAN_REF': plans_df,
        'SUBSCR_ACCT': subscribers_df,
        'SUBSCR_PLAN_ASSGN': plan_assgn_df,
        'CUST_ADDR': addresses_df,
        'CUST_CNTCT': contacts_df,
        'IDENT_DOC': ident_docs_df,
        'CUST_STAT_HIST': status_hist_df,
        'BLNG_ACCT': billing_accts_df,
        'INVC': invoices_df,
        'INVC_LN_ITEM': line_items_df,
        'PYMT': payments_df,
        'PYMT_MTHD': pymt_methods_df,
        'USAGE_REC': usage_df,
        'CDR_REC': cdr_df,
        'NTWK_ELEM': ntwk_elems_df,
        'CELL_TWR': cell_towers_df,
        'SVC_ORD': svc_orders_df,
        'SVC_ORD_ITEM': svc_ord_items_df,
        'WRK_ORD_ASSGN': wrk_ord_assgn_df,
        'NTWK_INCDT': ntwk_incidents_df,
        'FIELD_AGT': field_agents_df,
    }

    for table_name, df in table_map.items():
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
            logger.info(f"  {table_name}: {len(df)} records written")
        except Exception as e:
            logger.error(f"  Failed to write {table_name}: {e}")

    logger.info(f"Seed data generation complete. {len(table_map)} tables populated.")


if __name__ == '__main__':
    main()
