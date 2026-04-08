-- ============================================================
-- DOMAIN 1: Customer & Subscriber Management
-- 8 tables with abbreviation-heavy column names
-- Telecom enterprise schema with realistic constraints
-- ============================================================

-- Customer Master — core entity table
CREATE TABLE CUST_MSTR (
    CUST_ID             INTEGER PRIMARY KEY,
    CUST_FRST_NM        VARCHAR(100) NOT NULL,       -- First Name (PII)
    CUST_LST_NM         VARCHAR(100) NOT NULL,        -- Last Name (PII)
    CUST_MID_NM         VARCHAR(100),                 -- Middle Name (PII)
    CUST_DOB            DATE,                          -- Date of Birth (PII)
    CUST_SSN            VARCHAR(11),                   -- Social Security Number (PII-Critical)
    CUST_GNDR_CD        VARCHAR(1),                    -- Gender Code: M/F/X
    CUST_TEN_MNT        INTEGER,                       -- Tenure in Months
    CUST_SGMT_CD        VARCHAR(20),                   -- Segment Code: PLATINUM/GOLD/SILVER/BRONZE
    CUST_RISK_SCR       DECIMAL(5,2),                  -- Churn Risk Score (0-100)
    CUST_CRED_SCR       INTEGER,                       -- Credit Score (300-850)
    CUST_ACQSN_CHNL_CD  VARCHAR(20),                   -- Acquisition Channel Code
    CUST_ACQSN_DT       DATE,                          -- Acquisition Date
    CUST_STAT_CD        VARCHAR(10) NOT NULL,           -- Status Code: ACT/SUS/TRM/PDG
    CUST_STAT_RSN_CD    VARCHAR(20),                   -- Status Reason Code
    CUST_LANG_PREF_CD   VARCHAR(5) DEFAULT 'EN',       -- Language Preference
    CUST_CREAT_DT       DATETIME NOT NULL,             -- Record Created Date
    CUST_UPDT_DT        DATETIME NOT NULL,             -- Record Updated Date
    CUST_UPDT_BY        VARCHAR(50)                    -- Updated By (system/user)
);

-- Subscriber Account — links to customer, holds IMSI
CREATE TABLE SUBSCR_ACCT (
    SUBSCR_ID           INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    SUBSCR_IMSI_NO      VARCHAR(15),                   -- IMSI Number (PII-Telecom)
    SUBSCR_MSISDN_NO    VARCHAR(15),                   -- Phone Number (PII)
    SUBSCR_ICCID_NO     VARCHAR(22),                   -- SIM Card ID
    SUBSCR_ACCT_TYP_CD  VARCHAR(10) NOT NULL,           -- Account Type: PREPAID/POSTPAID/HYBRID
    SUBSCR_STAT_CD      VARCHAR(10) NOT NULL,           -- Status: ACT/SUS/TRM/PDG
    SUBSCR_ACTV_DT      DATE,                          -- Activation Date
    SUBSCR_DEACT_DT     DATE,                          -- Deactivation Date
    SUBSCR_DATA_PLAN_CD VARCHAR(20),                   -- Current Data Plan Code
    SUBSCR_MNT_USG_GB   DECIMAL(10,2),                 -- Monthly Usage in GB
    SUBSCR_MNT_VOICE_MIN INTEGER,                      -- Monthly Voice Minutes
    SUBSCR_ROAM_FLG     VARCHAR(1) DEFAULT 'N',        -- Roaming Flag: Y/N
    SUBSCR_CREAT_DT     DATETIME NOT NULL,
    SUBSCR_UPDT_DT      DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Subscriber Plan Assignment — junction table (subscriber <-> plans)
CREATE TABLE SUBSCR_PLAN_ASSGN (
    ASSGN_ID            INTEGER PRIMARY KEY,
    SUBSCR_ID           INTEGER NOT NULL,
    PLAN_CD             VARCHAR(20) NOT NULL,
    ASSGN_STRT_DT       DATE NOT NULL,                 -- Assignment Start Date
    ASSGN_END_DT        DATE,                          -- Assignment End Date (null = active)
    ASSGN_STAT_CD       VARCHAR(10) NOT NULL,           -- Status: ACT/EXP/CXL
    ASSGN_MNT_CHRG_AMT  DECIMAL(10,2),                 -- Monthly Charge Amount
    ASSGN_DISC_PCT      DECIMAL(5,2) DEFAULT 0,        -- Discount Percentage
    ASSGN_PROM_CD       VARCHAR(20),                   -- Promotion Code
    ASSGN_AUTO_RNW_FLG  VARCHAR(1) DEFAULT 'Y',        -- Auto Renewal Flag
    ASSGN_CREAT_DT      DATETIME NOT NULL,
    FOREIGN KEY (SUBSCR_ID) REFERENCES SUBSCR_ACCT(SUBSCR_ID),
    FOREIGN KEY (PLAN_CD) REFERENCES SVC_PLAN_REF(PLAN_CD)
);

-- Service Plan Reference — small reference table
CREATE TABLE SVC_PLAN_REF (
    PLAN_CD             VARCHAR(20) PRIMARY KEY,
    PLAN_NM             VARCHAR(100) NOT NULL,          -- Plan Name
    PLAN_TYP_CD         VARCHAR(20) NOT NULL,           -- Plan Type: VOICE/DATA/COMBO/IOT
    PLAN_TIER_CD        VARCHAR(10),                    -- Tier: BASIC/STANDARD/PREMIUM/UNLIMITED
    PLAN_MNT_COST_AMT   DECIMAL(10,2) NOT NULL,        -- Monthly Cost
    PLAN_DATA_LMT_GB    DECIMAL(10,2),                 -- Data Limit in GB (null = unlimited)
    PLAN_VOICE_LMT_MIN  INTEGER,                       -- Voice Limit in Minutes
    PLAN_SMS_LMT_CNT    INTEGER,                       -- SMS Limit Count
    PLAN_INTL_FLG       VARCHAR(1) DEFAULT 'N',        -- International Included
    PLAN_5G_FLG         VARCHAR(1) DEFAULT 'N',        -- 5G Capable
    PLAN_STRT_DT        DATE NOT NULL,                 -- Plan Available From
    PLAN_END_DT         DATE,                          -- Plan Discontinued Date
    PLAN_STAT_CD        VARCHAR(10) DEFAULT 'ACT'      -- Status: ACT/DISC/DEPR
);

-- Customer Address — multiple addresses per customer
CREATE TABLE CUST_ADDR (
    ADDR_ID             INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    ADDR_TYP_CD         VARCHAR(10) NOT NULL,           -- Type: HOME/WORK/BILLING/SHIPPING
    ADDR_LN_1           VARCHAR(200) NOT NULL,          -- Address Line 1 (PII)
    ADDR_LN_2           VARCHAR(200),                   -- Address Line 2
    ADDR_CITY_NM        VARCHAR(100) NOT NULL,          -- City (PII)
    ADDR_ST_CD          VARCHAR(5) NOT NULL,            -- State Code
    ADDR_ZIP_CD         VARCHAR(10) NOT NULL,           -- ZIP Code (PII)
    ADDR_CNTRY_CD       VARCHAR(3) DEFAULT 'US',       -- Country Code
    ADDR_PRI_FLG        VARCHAR(1) DEFAULT 'N',        -- Primary Address Flag
    ADDR_VALID_FLG      VARCHAR(1) DEFAULT 'Y',        -- Address Validated
    ADDR_CREAT_DT       DATETIME NOT NULL,
    ADDR_UPDT_DT        DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Customer Contact — phone, email, preferred contact
CREATE TABLE CUST_CNTCT (
    CNTCT_ID            INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    CNTCT_TYP_CD        VARCHAR(10) NOT NULL,           -- Type: PHONE/EMAIL/SMS/MAIL
    CNTCT_VAL           VARCHAR(200) NOT NULL,          -- Contact Value (PII - phone/email)
    CNTCT_PRI_FLG       VARCHAR(1) DEFAULT 'N',        -- Primary Contact Flag
    CNTCT_OPT_IN_FLG    VARCHAR(1) DEFAULT 'Y',        -- Marketing Opt-In
    CNTCT_VRFY_FLG      VARCHAR(1) DEFAULT 'N',        -- Verified Flag
    CNTCT_VRFY_DT       DATE,                          -- Verification Date
    CNTCT_CREAT_DT      DATETIME NOT NULL,
    CNTCT_UPDT_DT       DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Identity Document — PII-critical
CREATE TABLE IDENT_DOC (
    DOC_ID              INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    DOC_TYP_CD          VARCHAR(20) NOT NULL,           -- Type: SSN/PASSPORT/DL/STATE_ID
    DOC_NO              VARCHAR(50) NOT NULL,           -- Document Number (PII-Critical)
    DOC_ISS_DT          DATE,                          -- Issue Date
    DOC_EXP_DT          DATE,                          -- Expiry Date
    DOC_ISS_AUTH_NM     VARCHAR(100),                  -- Issuing Authority
    DOC_VRFY_STAT_CD    VARCHAR(10) DEFAULT 'PDG',     -- Verification Status: VRFY/PDG/FAIL
    DOC_CREAT_DT        DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Customer Status History — event table tracking status transitions
CREATE TABLE CUST_STAT_HIST (
    HIST_ID             INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    PREV_STAT_CD        VARCHAR(10),                   -- Previous Status
    NEW_STAT_CD         VARCHAR(10) NOT NULL,           -- New Status
    STAT_CHG_RSN_CD     VARCHAR(20),                   -- Change Reason Code
    STAT_CHG_DT         DATETIME NOT NULL,             -- Change Date/Time
    STAT_CHG_BY         VARCHAR(50),                   -- Changed By (agent/system)
    STAT_CHG_CHNL_CD    VARCHAR(20),                   -- Channel: WEB/CALL/STORE/SYSTEM
    STAT_CHG_NOTES      TEXT,                          -- Notes
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);
