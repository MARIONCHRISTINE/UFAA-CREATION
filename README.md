-- 1A. HIVE STAGING TABLE (Data)
CREATE TABLE hive.sre.ufaadata2025 (
    owner_code VARCHAR(20),
    owner_name VARCHAR(5000),
    owner_dob DATE,
    owner_id VARCHAR(100),
    owner_msisdn VARCHAR(20),
    transaction_date DATE,
    transaction_time VARCHAR(20),
    owner_due_amount DECIMAL(15, 2)
);   
-- 1B. ICEBERG PRODUCTION TABLE (Data)
CREATE TABLE iceberg.adhoc.ufaadata2025 (
    owner_code VARCHAR(20),
    owner_name VARCHAR(5000),
    owner_dob DATE,
    owner_id VARCHAR(100),
    owner_msisdn VARCHAR(20),
    transaction_date DATE,
    transaction_time VARCHAR(20),
    owner_due_amount DECIMAL(15, 2)
);
-- ========================================
-- 2. LETTER TRACKING TABLES (Sparse - only when letter sent)
-- ========================================
-- 2A. HIVE STAGING TABLE (Letters)
CREATE TABLE hive.sre.ufaaletters (
    owner_code VARCHAR(20),
    letter_sent VARCHAR(10),
    letter_date DATE,
    letter_ref_no VARCHAR(100)
);
-- 2B. ICEBERG PRODUCTION TABLE (Letters)
CREATE TABLE iceberg.adhoc.ufaaletters (
    owner_code VARCHAR(20),
    letter_sent VARCHAR(10),
    letter_date DATE,
    letter_ref_no VARCHAR(100)
);
