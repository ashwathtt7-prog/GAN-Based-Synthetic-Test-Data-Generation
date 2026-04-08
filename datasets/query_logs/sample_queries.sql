-- Simulated historical query logs for the Query Log Miner to extract implicit JOIN patterns

-- Query 1: Very common query joining Customer and Subscriber (Explicit FK)
SELECT c.CUST_FRST_NM, c.CUST_LST_NM, s.SUBSCR_MSISDN_NO
FROM CUST_MSTR c
JOIN SUBSCR_ACCT s ON c.CUST_ID = s.CUST_ID
WHERE s.SUBSCR_STAT_CD = 'ACT';

-- Query 2: Common query joining Invoice and Billing Account (Explicit FK)
SELECT i.INVC_NO, b.BLNG_ACCT_NO, i.INVC_TOT_AMT
FROM INVC i
INNER JOIN BLNG_ACCT b ON i.BLNG_ACCT_ID = b.BLNG_ACCT_ID
WHERE i.INVC_STAT_CD = 'OVERDUE';

-- Query 3: Implicit JOIN not formally constrained in DB (Usage to Billing)
-- This is what the log miner is designed to catch
SELECT u.USAGE_DUR_SEC, b.BLNG_TYP_CD
FROM USAGE_REC u
JOIN BLNG_ACCT b ON u.SUBSCR_ID = b.BLNG_ACCT_ID -- Conceptual relationship
WHERE b.BLNG_TYP_CD = 'CORP';

-- Query 4: Multi-table JOIN
SELECT c.CUST_ID, a.ADDR_CITY_NM, s.SUBSCR_ACTV_DT
FROM CUST_MSTR c
JOIN CUST_ADDR a ON c.CUST_ID = a.CUST_ID
JOIN SUBSCR_ACCT s ON c.CUST_ID = s.CUST_ID
WHERE a.ADDR_PRI_FLG = 'Y';
