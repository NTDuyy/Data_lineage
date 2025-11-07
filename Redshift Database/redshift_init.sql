-- Drop tables (in dependency order)
DROP TABLE IF EXISTS lineorder;
DROP TABLE IF EXISTS dwdate;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;

-- Create dimension tables
CREATE TABLE part (
  p_partkey     INTEGER NOT NULL,
  p_name        VARCHAR(100) NOT NULL,
  p_mfgr        VARCHAR(20),
  p_category    VARCHAR(20) NOT NULL,
  p_brand1      VARCHAR(30) NOT NULL,
  p_color       VARCHAR(20) NOT NULL,
  p_type        VARCHAR(30) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   VARCHAR(20) NOT NULL,
  PRIMARY KEY (p_partkey)
);

CREATE TABLE supplier (
  s_suppkey   INTEGER NOT NULL,
  s_name      VARCHAR(50) NOT NULL,
  s_address   VARCHAR(50) NOT NULL,
  s_city      VARCHAR(30) NOT NULL,
  s_nation    VARCHAR(30) NOT NULL,
  s_region    VARCHAR(30) NOT NULL,
  s_phone     VARCHAR(20) NOT NULL,
  PRIMARY KEY (s_suppkey)
);

CREATE TABLE customer (
  c_custkey      INTEGER NOT NULL,
  c_name         VARCHAR(50) NOT NULL,
  c_address      VARCHAR(50) NOT NULL,
  c_city         VARCHAR(30) NOT NULL,
  c_nation       VARCHAR(30) NOT NULL,
  c_region       VARCHAR(30) NOT NULL,
  c_phone        VARCHAR(20) NOT NULL,
  c_mktsegment   VARCHAR(20) NOT NULL,
  PRIMARY KEY (c_custkey)
);

CREATE TABLE dwdate (
  d_datekey            INTEGER NOT NULL,
  d_date               DATE NOT NULL,
  d_dayofweek          VARCHAR(10) NOT NULL,
  d_month              VARCHAR(10) NOT NULL,
  d_year               INTEGER NOT NULL,
  d_yearmonthnum       INTEGER NOT NULL,
  d_yearmonth          VARCHAR(10) NOT NULL,
  d_daynuminweek       INTEGER NOT NULL,
  d_daynuminmonth      INTEGER NOT NULL,
  d_daynuminyear       INTEGER NOT NULL,
  d_monthnuminyear     INTEGER NOT NULL,
  d_weeknuminyear      INTEGER NOT NULL,
  d_sellingseason      VARCHAR(20) NOT NULL,
  d_lastdayinweekfl    CHAR(1) NOT NULL,
  d_lastdayinmonthfl   CHAR(1) NOT NULL,
  d_holidayfl          CHAR(1) NOT NULL,
  d_weekdayfl          CHAR(1) NOT NULL,
  PRIMARY KEY (d_datekey)
);

-- Create fact table
CREATE TABLE lineorder (
  lo_orderkey          INTEGER NOT NULL,
  lo_linenumber        INTEGER NOT NULL,
  lo_custkey           INTEGER NOT NULL REFERENCES customer(c_custkey),
  lo_partkey           INTEGER NOT NULL REFERENCES part(p_partkey),
  lo_suppkey           INTEGER NOT NULL REFERENCES supplier(s_suppkey),
  lo_orderdate         INTEGER NOT NULL REFERENCES dwdate(d_datekey),
  lo_orderpriority     VARCHAR(20) NOT NULL,
  lo_shippriority      CHAR(1) NOT NULL,
  lo_quantity          INTEGER NOT NULL,
  lo_extendedprice     DECIMAL(12,2) NOT NULL,
  lo_ordertotalprice   DECIMAL(12,2) NOT NULL,
  lo_discount          DECIMAL(5,2) NOT NULL,
  lo_revenue           DECIMAL(12,2) NOT NULL,
  lo_supplycost        DECIMAL(12,2) NOT NULL,
  lo_tax               DECIMAL(5,2) NOT NULL,
  lo_commitdate        INTEGER NOT NULL,
  lo_shipmode          VARCHAR(10) NOT NULL,
  PRIMARY KEY (lo_orderkey)
);

-- Insert parts
INSERT INTO part VALUES
(1, 'Laptop Model A', 'MFGR1', 'ELEC', 'BrandX', 'Silver', 'Notebook', 15, 'Box'),
(2, 'Phone Model B', 'MFGR2', 'ELEC', 'BrandY', 'Black', 'Smartphone', 6, 'Bag'),
(3, 'Tablet Model C', 'MFGR1', 'ELEC', 'BrandZ', 'White', 'Tablet', 10, 'Box'),
(4, 'Monitor 24inch', 'MFGR3', 'ELEC', 'BrandX', 'Black', 'Display', 24, 'Box'),
(5, 'Keyboard K1', 'MFGR4', 'COMP', 'BrandY', 'Gray', 'Accessory', 5, 'Carton');

-- Insert suppliers
INSERT INTO supplier VALUES
(1, 'TechSupply Co.', '123 Elm St', 'Seattle', 'USA', 'North America', '206-111-1111'),
(2, 'GadgetWorld', '45 Pine Rd', 'Toronto', 'Canada', 'North America', '416-222-2222'),
(3, 'PartsHub', '78 Oak Ave', 'Berlin', 'Germany', 'Europe', '49-333-3333'),
(4, 'CompSpare Ltd', '12 River Rd', 'Tokyo', 'Japan', 'Asia', '81-444-4444'),
(5, 'SmartParts', '9 Bay Blvd', 'Sydney', 'Australia', 'Oceania', '61-555-5555');

-- Insert customers
INSERT INTO customer VALUES
(1, 'Alice Corp', '10 Maple St', 'Seattle', 'USA', 'North America', '206-111-1112', 'Retail'),
(2, 'Bob Trading', '22 Lake Rd', 'Toronto', 'Canada', 'North America', '416-222-1111', 'Wholesale'),
(3, 'Cathy Tech', '77 Hill Ave', 'Berlin', 'Germany', 'Europe', '49-333-2222', 'Online'),
(4, 'David Stores', '66 Park Ln', 'Tokyo', 'Japan', 'Asia', '81-444-3333', 'Retail'),
(5, 'Eva Supply', '33 Ocean Dr', 'Sydney', 'Australia', 'Oceania', '61-555-4444', 'Wholesale');

-- Insert date dimension
INSERT INTO dwdate VALUES
(20230101, '2023-01-01', 'Sunday', 'January', 2023, 202301, '2023-01', 1, 1, 1, 1, 1, 'Winter', 'N', 'N', 'Y', 'N'),
(20230102, '2023-01-02', 'Monday', 'January', 2023, 202301, '2023-01', 2, 2, 2, 1, 1, 'Winter', 'N', 'N', 'N', 'Y'),
(20230103, '2023-01-03', 'Tuesday', 'January', 2023, 202301, '2023-01', 3, 3, 3, 1, 1, 'Winter', 'N', 'N', 'N', 'Y'),
(20230201, '2023-02-01', 'Wednesday', 'February', 2023, 202302, '2023-02', 3, 1, 32, 2, 5, 'Winter', 'N', 'N', 'N', 'Y'),
(20230301, '2023-03-01', 'Wednesday', 'March', 2023, 202303, '2023-03', 3, 1, 60, 3, 9, 'Spring', 'N', 'N', 'N', 'Y');

-- Insert line orders
INSERT INTO lineorder VALUES
(1001, 1, 1, 1, 1, 20230101, 'HIGH', '1', 2, 2000.00, 4000.00, 10.00, 3600.00, 1500.00, 5.00, 20230102, 'AIR'),
(1002, 1, 2, 2, 2, 20230102, 'MEDIUM', '1', 1, 1000.00, 1000.00, 5.00, 950.00, 500.00, 2.00, 20230103, 'SHIP'),
(1003, 2, 3, 3, 3, 20230201, 'LOW', '1', 3, 3000.00, 9000.00, 20.00, 7200.00, 2500.00, 10.00, 20230301, 'RAIL'),
(1004, 3, 4, 4, 4, 20230301, 'HIGH', '1', 5, 5000.00, 25000.00, 15.00, 21250.00, 4000.00, 8.00, 20230301, 'TRUCK'),
(1005, 4, 5, 5, 5, 20230103, 'LOW', '1', 4, 4000.00, 16000.00, 10.00, 14400.00, 3000.00, 6.00, 20230201, 'AIR');


CREATE OR REPLACE PROCEDURE update_data()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lineorder
    SET lo_quantity = lo_quantity + 1;

    delete from customer
    where c_custkey = 1;

    INSERT INTO customer VALUES
    (1, 'Alice Corp', '10 Maple St', 'Seattle', 'USA', 'North America', '206-111-1112', 'Retail');
END;
$$;