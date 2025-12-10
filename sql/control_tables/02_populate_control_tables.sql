SET NOCOUNT ON;

-- A. Populate Source Systems
INSERT INTO control.source_systems (source_system_id, system_name, system_description, is_active)
VALUES
('SRC-001', 'Azure SQL ERP', 'Orders, Inventory, Finance domain', 1),
('SRC-002', 'Azure SQL CRM', 'Customer Master, Support domain', 1),
('SRC-003', 'Azure SQL Marketing', 'Campaign Management domain', 1);

-- B. Populate Table Metadata
-- Load Priority logic:
-- 10: Master data / No dependencies
-- 20: Level 1 dependencies
-- 30: Level 2 dependencies
-- 40: Transactional data with deep dependencies

INSERT INTO control.table_metadata
(table_id, source_system_id, schema_name, table_name, entity_name, primary_key_columns, is_composite_key, load_priority)
VALUES
-- CRM System (SRC-002)
('ENT-001', 'SRC-002', 'CRM', 'Customers', 'Customer', 'CUSTOMER_ID', 0, 10),
('ENT-002', 'SRC-002', 'CRM', 'CustomerRegistrationSource', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID', 0, 30),
('ENT-011', 'SRC-002', 'CRM', 'INCIDENTS', 'INCIDENTS', 'INCIDENT_ID', 0, 40),
('ENT-012', 'SRC-002', 'CRM', 'INTERACTIONS', 'INTERACTIONS', 'INTERACTION_ID', 0, 50),
('ENT-013', 'SRC-002', 'CRM', 'SURVEYS', 'SURVEYS', 'SURVEY_ID', 0, 50),

-- ERP System (SRC-001)
('ENT-003', 'SRC-001', 'ERP', 'OE_ORDER_HEADERS_ALL', 'Order Header', 'ORDER_ID', 0, 30),
('ENT-004', 'SRC-001', 'ERP', 'OE_ORDER_LINES_ALL', 'Order Line', 'LINE_ID', 0, 40),
('ENT-005', 'SRC-001', 'ERP', 'ADDRESSES', 'Address', 'ADDRESS_ID', 0, 20),
('ENT-006', 'SRC-001', 'ERP', 'CITY_TIER_MASTER', 'City Tier', 'CITY,STATE', 1, 10), -- Composite Key
('ENT-007', 'SRC-001', 'ERP', 'MTL_SYSTEM_ITEMS_B', 'Product', 'INVENTORY_ITEM_ID', 0, 20),
('ENT-008', 'SRC-001', 'ERP', 'CATEGORIES', 'Category', 'CATEGORY_ID', 0, 10),
('ENT-009', 'SRC-001', 'ERP', 'BRANDS', 'Brand', 'BRAND_ID', 0, 10),

-- Marketing System (SRC-003)
('ENT-010', 'SRC-003', 'MARKETING', 'MARKETING_CAMPAIGNS', 'Campaign', 'CAMPAIGN_ID', 0, 10);

-- C. Populate Load Dependencies
-- CRM.CustomerRegistrationSource depends on CRM.Customers and MARKETING.MARKETING_CAMPAIGNS
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-002');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-010', 'ENT-002');

-- CRM.INCIDENTS depends on CRM.Customers and ERP.OE_ORDER_HEADERS_ALL
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-011');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-003', 'ENT-011');

-- CRM.INTERACTIONS depends on CRM.INCIDENTS and CRM.Customers
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-011', 'ENT-012');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-012');

-- CRM.SURVEYS depends on CRM.Customers, ERP.ORDER_HEADERS, CRM.INCIDENTS
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-013');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-003', 'ENT-013');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-011', 'ENT-013');

-- ERP.OE_ORDER_HEADERS_ALL depends on CRM.Customers and ERP.ADDRESSES
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-003');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-005', 'ENT-003');

-- ERP.OE_ORDER_LINES_ALL depends on ERP.OE_ORDER_HEADERS_ALL and ERP.MTL_SYSTEM_ITEMS_B
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-003', 'ENT-004');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-007', 'ENT-004');

-- ERP.ADDRESSES depends on CRM.Customers
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-001', 'ENT-005');
-- Note: ERP.ADDRESSES implicitly depends on CITY_TIER_MASTER via logic, adding explicit dependency
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-006', 'ENT-005');

-- ERP.MTL_SYSTEM_ITEMS_B depends on ERP.CATEGORIES and ERP.BRANDS
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-008', 'ENT-007');
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES ('ENT-009', 'ENT-007');

-- ERP.CATEGORIES self-reference handled by application logic (not a load blocking dependency usually unless strict hierarchy required)

-- D. Populate Data Quality Rules (Samples from Metadata)
INSERT INTO control.data_quality_rules (rule_id, table_id, rule_name, rule_type, column_name, rule_expression)
VALUES
('DQ-C-001', 'ENT-001', 'Email Required', 'Completeness', 'EMAIL', 'EMAIL IS NOT NULL'),
('DQ-C-003', 'ENT-003', 'Customer ID Required', 'Completeness', 'CUSTOMER_ID', 'CUSTOMER_ID IS NOT NULL'),
('DQ-V-001', 'ENT-003', 'Total Amount Positive', 'Validity', 'TOTAL_AMOUNT', 'TOTAL_AMOUNT > 0'),
('DQ-V-002', 'ENT-013', 'NPS Score Range', 'Validity', 'NPS_SCORE', 'NPS_SCORE BETWEEN 0 AND 10');
GO
