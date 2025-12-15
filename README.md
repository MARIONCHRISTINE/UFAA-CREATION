-- 1. HIVE STAGING TABLE
CREATE TABLE hive.sre.UFAA2_23203159 (
    owner_name STRING,
    owner_dob DATE,
    owner_id VARCHAR(100),
    transaction_date DATE,
    transaction_time VARCHAR(20),
    owner_due_amount DECIMAL(15, 2),
    letter_sent VARCHAR(10),
    letter_date DATE,
    letter_ref_no VARCHAR(100)
);

-- 2. ICEBERG PRODUCTION TABLE
CREATE TABLE iceberg.adhoc.ufaa2_23203159 (
    owner_name STRING,
    owner_dob DATE,
    owner_id VARCHAR(100),
    transaction_date DATE,
    transaction_time VARCHAR(20),
    owner_due_amount DECIMAL(15, 2),
    letter_sent VARCHAR(10),
    letter_date DATE,
    letter_ref_no VARCHAR(100)
);
