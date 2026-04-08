-- ============================================================
-- DOMAIN 3: Billing & Revenue
-- 7 tables with financial data, bimodal distributions, PII
-- Mission-critical for fraud detection and revenue assurance
-- ============================================================

-- Billing Account — links to CUST_MSTR
CREATE TABLE BLNG_ACCT (
    BLNG_ACCT_ID        INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    BLNG_ACCT_NO        VARCHAR(20) NOT NULL,           -- Billing Account Number
    BLNG_TYP_CD         VARCHAR(10) NOT NULL,           -- Type: IND/CORP/GOV/RESELLER
    BLNG_CYC_CD         VARCHAR(5) NOT NULL,            -- Billing Cycle: 01-28 (day of month)
    BLNG_PYMT_TERM_CD   VARCHAR(10) DEFAULT 'NET30',   -- Payment Terms: NET15/NET30/NET45/NET60
    BLNG_CRED_LMT_AMT   DECIMAL(12,2),                 -- Credit Limit
    BLNG_CURR_BAL_AMT   DECIMAL(12,2) DEFAULT 0,       -- Current Balance
    BLNG_PAST_DUE_AMT   DECIMAL(12,2) DEFAULT 0,       -- Past Due Amount
    BLNG_AUTOPAY_FLG    VARCHAR(1) DEFAULT 'N',        -- Auto-Pay Enabled
    BLNG_PPRLSS_FLG     VARCHAR(1) DEFAULT 'N',        -- Paperless Billing
    BLNG_STAT_CD        VARCHAR(10) DEFAULT 'ACT',     -- Status: ACT/SUS/CLS/COL (collections)
    BLNG_CREAT_DT       DATETIME NOT NULL,
    BLNG_UPDT_DT        DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Invoice — monthly billing cycles
CREATE TABLE INVC (
    INVC_ID             INTEGER PRIMARY KEY,
    BLNG_ACCT_ID        INTEGER NOT NULL,
    INVC_NO             VARCHAR(20) NOT NULL,           -- Invoice Number
    INVC_CYC_DT         DATE NOT NULL,                 -- Billing Cycle Date
    INVC_DUE_DT         DATE NOT NULL,                 -- Due Date (must be > INVC_CYC_DT)
    INVC_SUBTOT_AMT     DECIMAL(12,2) NOT NULL,        -- Subtotal Amount
    INVC_TAX_AMT        DECIMAL(12,2) DEFAULT 0,       -- Tax Amount
    INVC_DISC_AMT       DECIMAL(12,2) DEFAULT 0,       -- Discount Amount
    INVC_TOT_AMT        DECIMAL(12,2) NOT NULL,        -- Total = SUBTOT + TAX - DISC
    INVC_PAID_AMT       DECIMAL(12,2) DEFAULT 0,       -- Amount Paid
    INVC_BAL_AMT        DECIMAL(12,2),                 -- Outstanding Balance = TOT - PAID
    INVC_STAT_CD        VARCHAR(10) NOT NULL,           -- Status: DRAFT/ISSUED/PAID/PARTIAL/OVERDUE/VOID
    INVC_ISS_DT         DATE,                          -- Issue Date
    INVC_PAID_DT        DATE,                          -- Paid Date
    INVC_CREAT_DT       DATETIME NOT NULL,
    FOREIGN KEY (BLNG_ACCT_ID) REFERENCES BLNG_ACCT(BLNG_ACCT_ID)
);

-- Invoice Line Items — largest table, amount distributions
CREATE TABLE INVC_LN_ITEM (
    LN_ITEM_ID          INTEGER PRIMARY KEY,
    INVC_ID             INTEGER NOT NULL,
    LN_ITEM_SEQ_NO      INTEGER NOT NULL,              -- Sequence within invoice
    LN_ITEM_TYP_CD      VARCHAR(20) NOT NULL,           -- Type: MRC/USAGE/OTC/ADJ/CREDIT/TAX/SURCHARGE
    LN_ITEM_DESC        VARCHAR(200),                  -- Description
    LN_ITEM_SVC_STRT_DT DATE,                          -- Service Period Start
    LN_ITEM_SVC_END_DT  DATE,                          -- Service Period End
    LN_ITEM_QTY         DECIMAL(12,4) DEFAULT 1,       -- Quantity (can be fractional for usage)
    LN_ITEM_UNIT_PRC_AMT DECIMAL(12,4),                -- Unit Price
    LN_ITEM_DISC_PCT    DECIMAL(5,2) DEFAULT 0,        -- Discount Percentage
    LN_ITEM_DISC_AMT    DECIMAL(12,2) DEFAULT 0,       -- Discount Amount
    LN_ITEM_TAX_AMT     DECIMAL(12,2) DEFAULT 0,       -- Tax Amount
    LN_ITEM_TOT_AMT     DECIMAL(12,2) NOT NULL,        -- Line Total
    LN_ITEM_GL_ACCT_CD  VARCHAR(20),                   -- GL Account Code
    LN_ITEM_CREAT_DT    DATETIME NOT NULL,
    FOREIGN KEY (INVC_ID) REFERENCES INVC(INVC_ID)
);

-- Payment — bimodal distribution (full vs partial payments)
CREATE TABLE PYMT (
    PYMT_ID             INTEGER PRIMARY KEY,
    BLNG_ACCT_ID        INTEGER NOT NULL,
    PYMT_MTHD_ID        INTEGER,
    INVC_ID             INTEGER,                       -- Optional: payment applied to specific invoice
    PYMT_AMT            DECIMAL(12,2) NOT NULL,        -- Payment Amount
    PYMT_DT             DATETIME NOT NULL,             -- Payment Date
    PYMT_TYP_CD         VARCHAR(10) NOT NULL,           -- Type: FULL/PARTIAL/OVERPAY/REFUND/ADJUSTMENT
    PYMT_STAT_CD        VARCHAR(10) NOT NULL,           -- Status: PROC/COMP/FAIL/RVRS/PDG
    PYMT_CHNL_CD        VARCHAR(20),                   -- Channel: ONLINE/APP/PHONE/MAIL/STORE/AUTOPAY
    PYMT_CONF_NO        VARCHAR(30),                   -- Confirmation Number
    PYMT_FAIL_RSN_CD    VARCHAR(20),                   -- Failure Reason Code
    PYMT_PROC_DT        DATETIME,                      -- Processing Date
    PYMT_CREAT_DT       DATETIME NOT NULL,
    FOREIGN KEY (BLNG_ACCT_ID) REFERENCES BLNG_ACCT(BLNG_ACCT_ID),
    FOREIGN KEY (PYMT_MTHD_ID) REFERENCES PYMT_MTHD(PYMT_MTHD_ID),
    FOREIGN KEY (INVC_ID) REFERENCES INVC(INVC_ID)
);

-- Payment Method — PII (card numbers, bank accounts)
CREATE TABLE PYMT_MTHD (
    PYMT_MTHD_ID        INTEGER PRIMARY KEY,
    CUST_ID             INTEGER NOT NULL,
    MTHD_TYP_CD         VARCHAR(20) NOT NULL,           -- Type: CREDIT_CARD/DEBIT_CARD/ACH/CHECK/DIGITAL_WALLET
    MTHD_CARD_NO        VARCHAR(20),                   -- Card Number (PII - last 4 shown)
    MTHD_CARD_BRAND_CD  VARCHAR(10),                   -- Brand: VISA/MC/AMEX/DISC
    MTHD_CARD_EXP_MM    INTEGER,                       -- Expiry Month
    MTHD_CARD_EXP_YY    INTEGER,                       -- Expiry Year
    MTHD_BNK_ROUT_NO    VARCHAR(9),                    -- Bank Routing Number (PII)
    MTHD_BNK_ACCT_NO    VARCHAR(20),                   -- Bank Account Number (PII)
    MTHD_BNK_NM         VARCHAR(100),                  -- Bank Name
    MTHD_DGTL_WLLT_ID   VARCHAR(100),                  -- Digital Wallet ID
    MTHD_NICKM          VARCHAR(50),                   -- User-assigned nickname
    MTHD_PRI_FLG        VARCHAR(1) DEFAULT 'N',        -- Primary Method
    MTHD_STAT_CD        VARCHAR(10) DEFAULT 'ACT',     -- Status: ACT/EXP/SUS/DEL
    MTHD_CREAT_DT       DATETIME NOT NULL,
    MTHD_UPDT_DT        DATETIME NOT NULL,
    FOREIGN KEY (CUST_ID) REFERENCES CUST_MSTR(CUST_ID)
);

-- Usage Record — CDR-like, time-of-day patterns
CREATE TABLE USAGE_REC (
    USAGE_ID            INTEGER PRIMARY KEY,
    SUBSCR_ID           INTEGER NOT NULL,
    USAGE_TYP_CD        VARCHAR(10) NOT NULL,           -- Type: VOICE/DATA/SMS/MMS/ROAM_VOICE/ROAM_DATA
    USAGE_STRT_DT       DATETIME NOT NULL,             -- Start Time
    USAGE_END_DT        DATETIME,                      -- End Time
    USAGE_DUR_SEC       INTEGER,                       -- Duration in seconds (voice)
    USAGE_DATA_KB       DECIMAL(15,2),                 -- Data consumed in KB
    USAGE_ORIG_NO       VARCHAR(15),                   -- Originating Number (PII)
    USAGE_DEST_NO       VARCHAR(15),                   -- Destination Number (PII)
    USAGE_CELL_ID       VARCHAR(20),                   -- Cell Tower ID
    USAGE_ROAM_FLG      VARCHAR(1) DEFAULT 'N',
    USAGE_INTL_FLG      VARCHAR(1) DEFAULT 'N',        -- International Call/Data
    USAGE_CHRG_AMT      DECIMAL(10,4),                 -- Charge Amount
    USAGE_RATE_PLAN_CD  VARCHAR(20),                   -- Rate Plan applied
    USAGE_CREAT_DT      DATETIME NOT NULL,
    FOREIGN KEY (SUBSCR_ID) REFERENCES SUBSCR_ACCT(SUBSCR_ID)
);

-- Call Detail Records — mission-critical for fraud detection
CREATE TABLE CDR_REC (
    CDR_ID              INTEGER PRIMARY KEY,
    SUBSCR_ID           INTEGER NOT NULL,
    CDR_SEQ_NO          BIGINT NOT NULL,               -- Sequence Number
    CDR_TYP_CD          VARCHAR(10) NOT NULL,           -- Type: MOC/MTC/MOF/MTF/MOSMS/MTSMS (Originating/Terminating Call/Forward/SMS)
    CDR_ORIG_MSISDN     VARCHAR(15) NOT NULL,          -- Originating MSISDN (PII)
    CDR_DEST_MSISDN     VARCHAR(15) NOT NULL,          -- Destination MSISDN (PII)
    CDR_ORIG_IMSI       VARCHAR(15),                   -- Originating IMSI (PII)
    CDR_STRT_DT         DATETIME NOT NULL,             -- Call Start Time
    CDR_END_DT          DATETIME,                      -- Call End Time
    CDR_DUR_SEC         INTEGER NOT NULL,              -- Duration in seconds
    CDR_ORIG_CELL_ID    VARCHAR(20),                   -- Originating Cell
    CDR_DEST_CELL_ID    VARCHAR(20),                   -- Destination Cell
    CDR_CALL_RSLT_CD    VARCHAR(10),                   -- Result: COMP/DROP/BUSY/NOANS/FAIL
    CDR_ROAM_IND_CD     VARCHAR(5),                    -- Roaming Indicator: HOME/NAT/INTL
    CDR_CHRG_AMT        DECIMAL(10,4),                 -- Charge Amount
    CDR_RATED_DUR_SEC   INTEGER,                       -- Rated Duration (may differ from actual)
    CDR_FRAUD_SCR       DECIMAL(5,2),                  -- Fraud Score (0-100) — critical for AI pipeline
    CDR_FRAUD_FLG       VARCHAR(1) DEFAULT 'N',        -- Fraud Flag
    CDR_CREAT_DT        DATETIME NOT NULL,
    FOREIGN KEY (SUBSCR_ID) REFERENCES SUBSCR_ACCT(SUBSCR_ID)
);
