-- ============================================================
-- DOMAIN 2: Network & Service Operations
-- 7 tables with network infrastructure and service order management
-- ============================================================

-- Network Element — core network infrastructure
CREATE TABLE NTWK_ELEM (
    ELEM_ID             INTEGER PRIMARY KEY,
    ELEM_NM             VARCHAR(100) NOT NULL,          -- Element Name
    ELEM_TYP_CD         VARCHAR(20) NOT NULL,           -- Type: ROUTER/SWITCH/BTS/ENODEB/GNODEB/MSC
    ELEM_VNDR_NM        VARCHAR(50),                   -- Vendor Name: ERICSSON/NOKIA/HUAWEI/SAMSUNG
    ELEM_MDL_NO         VARCHAR(50),                   -- Model Number
    ELEM_SRIAL_NO       VARCHAR(50),                   -- Serial Number (sensitive)
    ELEM_LAT_COORD      DECIMAL(10,7),                 -- Latitude
    ELEM_LON_COORD      DECIMAL(10,7),                 -- Longitude
    ELEM_SITE_CD        VARCHAR(20),                   -- Site Code
    ELEM_RGN_CD         VARCHAR(10),                   -- Region Code: NE/SE/MW/SW/NW/WC
    ELEM_CAP_PCT        DECIMAL(5,2),                  -- Current Capacity Utilization %
    ELEM_MAX_CONN_CNT   INTEGER,                       -- Max Connections
    ELEM_CURR_CONN_CNT  INTEGER,                       -- Current Connections
    ELEM_FRWRE_VER      VARCHAR(30),                   -- Firmware Version
    ELEM_INSTL_DT       DATE,                          -- Installation Date
    ELEM_LST_MAINT_DT   DATE,                          -- Last Maintenance Date
    ELEM_STAT_CD        VARCHAR(10) DEFAULT 'OPER',    -- Status: OPER/MAINT/FAIL/DECOM
    ELEM_CREAT_DT       DATETIME NOT NULL,
    ELEM_UPDT_DT        DATETIME NOT NULL
);

-- Cell Tower — linked to network elements
CREATE TABLE CELL_TWR (
    TWR_ID              INTEGER PRIMARY KEY,
    ELEM_ID             INTEGER NOT NULL,
    TWR_NM              VARCHAR(100),                  -- Tower Name
    TWR_TYP_CD          VARCHAR(20) NOT NULL,           -- Type: MACRO/MICRO/PICO/FEMTO/SMALL_CELL
    TWR_HGT_FT          DECIMAL(8,2),                  -- Height in feet
    TWR_FREQ_BAND_CD    VARCHAR(20),                   -- Frequency Band: 700MHZ/850MHZ/1900MHZ/2100MHZ/3500MHZ/MMWAVE
    TWR_TECH_CD         VARCHAR(10),                   -- Technology: 3G/4G/5G/5G_SA
    TWR_CVG_RAD_MI      DECIMAL(8,2),                  -- Coverage Radius in miles
    TWR_SECT_CNT        INTEGER DEFAULT 3,             -- Number of sectors
    TWR_PWR_SRC_CD      VARCHAR(10),                   -- Power Source: GRID/SOLAR/HYBRID/GENERATOR
    TWR_BKUP_PWR_HRS    DECIMAL(5,1),                  -- Backup Power Duration hours
    TWR_OWNR_TYP_CD     VARCHAR(10),                   -- Owner Type: OWNED/LEASED/SHARED
    TWR_LEASE_EXP_DT    DATE,                          -- Lease Expiry Date
    TWR_STAT_CD         VARCHAR(10) DEFAULT 'OPER',    -- Status: OPER/MAINT/PLAN/DECOM
    TWR_CREAT_DT        DATETIME NOT NULL,
    FOREIGN KEY (ELEM_ID) REFERENCES NTWK_ELEM(ELEM_ID)
);

-- Service Order — temporal chain: ORDER_DT → PROV_DT → ACTV_DT
CREATE TABLE SVC_ORD (
    ORD_ID              INTEGER PRIMARY KEY,
    SUBSCR_ID           INTEGER NOT NULL,
    ORD_TYP_CD          VARCHAR(20) NOT NULL,           -- Type: NEW_ACT/UPGRADE/DOWNGRADE/SUSPEND/TERMINATE/PORT_IN/PORT_OUT
    ORD_STAT_CD         VARCHAR(10) NOT NULL,           -- Status: NEW/PROC/PROV/ACTV/COMP/CXL/FAIL
    ORD_PRI_CD          VARCHAR(10) DEFAULT 'NORM',    -- Priority: LOW/NORM/HIGH/URGENT/ESCL
    ORD_CREAT_DT        DATETIME NOT NULL,             -- Order Created Date
    ORD_SUBMIT_DT       DATETIME,                      -- Order Submitted Date
    ORD_PROV_DT         DATETIME,                      -- Provisioning Date (must be >= SUBMIT_DT)
    ORD_ACTV_DT         DATETIME,                      -- Activation Date (must be >= PROV_DT)
    ORD_COMP_DT         DATETIME,                      -- Completion Date (must be >= ACTV_DT)
    ORD_CXL_DT          DATETIME,                      -- Cancellation Date
    ORD_CXL_RSN_CD      VARCHAR(20),                   -- Cancellation Reason
    ORD_SRC_CHNL_CD     VARCHAR(20),                   -- Source Channel: WEB/APP/STORE/CALL/PARTNER
    ORD_AGT_ID          VARCHAR(20),                   -- Agent ID who created
    ORD_TOT_AMT         DECIMAL(10,2),                 -- Total Order Amount
    ORD_DISC_AMT        DECIMAL(10,2) DEFAULT 0,       -- Discount Amount
    ORD_TAX_AMT         DECIMAL(10,2) DEFAULT 0,       -- Tax Amount
    ORD_NOTES           TEXT,
    FOREIGN KEY (SUBSCR_ID) REFERENCES SUBSCR_ACCT(SUBSCR_ID)
);

-- Service Order Line Items
CREATE TABLE SVC_ORD_ITEM (
    ITEM_ID             INTEGER PRIMARY KEY,
    ORD_ID              INTEGER NOT NULL,
    PLAN_CD             VARCHAR(20),
    ITEM_TYP_CD         VARCHAR(20) NOT NULL,           -- Type: PLAN/DEVICE/ACCESSORY/FEE/CREDIT
    ITEM_DESC           VARCHAR(200),
    ITEM_QTY            INTEGER DEFAULT 1,
    ITEM_UNIT_PRC_AMT   DECIMAL(10,2),                 -- Unit Price
    ITEM_TOT_AMT        DECIMAL(10,2),                 -- Total = QTY * UNIT_PRC
    ITEM_DISC_AMT       DECIMAL(10,2) DEFAULT 0,
    ITEM_TAX_AMT        DECIMAL(10,2) DEFAULT 0,
    ITEM_STAT_CD        VARCHAR(10) DEFAULT 'PDG',     -- Status: PDG/PROC/COMP/CXL
    ITEM_CREAT_DT       DATETIME NOT NULL,
    FOREIGN KEY (ORD_ID) REFERENCES SVC_ORD(ORD_ID),
    FOREIGN KEY (PLAN_CD) REFERENCES SVC_PLAN_REF(PLAN_CD)
);

-- Work Order Assignment — junction: field agent <-> service orders
CREATE TABLE WRK_ORD_ASSGN (
    ASSGN_ID            INTEGER PRIMARY KEY,
    ORD_ID              INTEGER NOT NULL,
    AGT_ID              INTEGER NOT NULL,
    ASSGN_TYP_CD        VARCHAR(20),                   -- Type: INSTALL/REPAIR/INSPECT/DECOMMISSION
    ASSGN_STAT_CD       VARCHAR(10) NOT NULL,           -- Status: ASSGN/ENROUTE/ONSITE/COMP/CXL
    ASSGN_PRI_CD        VARCHAR(10) DEFAULT 'NORM',
    ASSGN_SCHD_DT       DATETIME,                      -- Scheduled Date
    ASSGN_STRT_DT       DATETIME,                      -- Actual Start
    ASSGN_COMP_DT       DATETIME,                      -- Actual Completion
    ASSGN_DUR_MIN       INTEGER,                       -- Duration in minutes
    ASSGN_TRAVEL_MI     DECIMAL(8,2),                  -- Travel Distance miles
    ASSGN_NOTES         TEXT,
    ASSGN_CUST_RTNG     INTEGER,                       -- Customer Rating 1-5
    ASSGN_CREAT_DT      DATETIME NOT NULL,
    FOREIGN KEY (ORD_ID) REFERENCES SVC_ORD(ORD_ID),
    FOREIGN KEY (AGT_ID) REFERENCES FIELD_AGT(AGT_ID)
);

-- Network Incident — severity tracking, impacted elements
CREATE TABLE NTWK_INCDT (
    INCDT_ID            INTEGER PRIMARY KEY,
    ELEM_ID             INTEGER NOT NULL,
    INCDT_TYP_CD        VARCHAR(20) NOT NULL,           -- Type: OUTAGE/DEGRADATION/ALARM/SECURITY/MAINTENANCE
    INCDT_SVRTY_CD      VARCHAR(10) NOT NULL,           -- Severity: P1/P2/P3/P4
    INCDT_STAT_CD       VARCHAR(10) NOT NULL,           -- Status: OPEN/INVEST/RESOLVE/CLOSED
    INCDT_IMPCT_LVL_CD  VARCHAR(10),                   -- Impact Level: CRITICAL/MAJOR/MINOR/COSMETIC
    INCDT_CUST_IMPCT_CNT INTEGER DEFAULT 0,            -- Number of customers impacted
    INCDT_RPT_DT        DATETIME NOT NULL,             -- Reported Date
    INCDT_ACK_DT        DATETIME,                      -- Acknowledged Date
    INCDT_RSL_DT        DATETIME,                      -- Resolution Date
    INCDT_CLS_DT        DATETIME,                      -- Closed Date
    INCDT_TTR_MIN       INTEGER,                       -- Time to Resolve in minutes
    INCDT_ROOT_CAUSE_CD VARCHAR(30),                   -- Root Cause Code
    INCDT_DESC          TEXT,
    INCDT_RSL_NOTES     TEXT,
    INCDT_CREAT_DT      DATETIME NOT NULL,
    FOREIGN KEY (ELEM_ID) REFERENCES NTWK_ELEM(ELEM_ID)
);

-- Field Agent — small entity table
CREATE TABLE FIELD_AGT (
    AGT_ID              INTEGER PRIMARY KEY,
    AGT_FRST_NM         VARCHAR(100) NOT NULL,          -- First Name (PII)
    AGT_LST_NM          VARCHAR(100) NOT NULL,          -- Last Name (PII)
    AGT_EMP_ID          VARCHAR(20) NOT NULL,           -- Employee ID
    AGT_EMAIL           VARCHAR(200),                   -- Email (PII)
    AGT_PHN_NO          VARCHAR(15),                   -- Phone (PII)
    AGT_CERT_LST        VARCHAR(500),                  -- Certifications (comma-separated)
    AGT_SKILL_LVL_CD    VARCHAR(10),                   -- Skill Level: JNR/MID/SNR/LEAD
    AGT_RGN_CD          VARCHAR(10),                   -- Assigned Region
    AGT_AVAIL_STAT_CD   VARCHAR(10) DEFAULT 'AVAIL',   -- Availability: AVAIL/BUSY/OFF/LEAVE
    AGT_HIRE_DT         DATE,                          -- Hire Date
    AGT_STAT_CD         VARCHAR(10) DEFAULT 'ACT',     -- Status: ACT/SUS/TRM
    AGT_CREAT_DT        DATETIME NOT NULL
);
