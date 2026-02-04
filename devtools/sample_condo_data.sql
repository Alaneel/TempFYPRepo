-- Insert sample condo_basic data into real_estate_app database
-- This uses the new table structure with 'id' as primary key

-- Record 1: # 1 Loft
INSERT INTO condo_basic (condo_name, developer_name, street_name, postal_code, latitude, longitude, tenure, total_units, district, mrt_nearby, has_swimming_pool, has_gym, has_tennis_court, has_security, has_parking, property_type, top_date, neighbourhood, num_floors, num_blocks, amenities_json, facilities_json)
VALUES ('# 1 Loft', NULL, '1 Lorong 24 Geylang', '398614', 1.312763, 103.883519, 'Freehold', 80, 14, 'Aljunied MRT · 5 min walk', TRUE, TRUE, FALSE, FALSE, FALSE, 'Condo Apartment', '1 Jan 2016', 'Geylang', 8, 1, 
'[{"name": "Yuan Zheng Tang Of True Buddha School", "distance": "5 mins (311 m)"}]',
'["Fitness Corner", "Pool Deck", "Swimming Pool"]');

-- Record 2: # 1 Suites
INSERT INTO condo_basic (condo_name, developer_name, street_name, postal_code, latitude, longitude, tenure, total_units, district, mrt_nearby, has_swimming_pool, has_gym, has_tennis_court, has_security, has_parking, property_type, top_date, neighbourhood, num_floors, num_blocks, amenities_json, facilities_json)
VALUES ('# 1 Suites', 'The One Development Pte. Ltd', '1 Lorong 20 Geylang', '398721', 1.312390, 103.881504, 'Freehold', 112, 14, 'Aljunied MRT · 6 min walk', TRUE, TRUE, FALSE, TRUE, TRUE, 'Condo Apartment', '1 Jan 2018', 'Geylang', 8, 1,
'[{"name": "HFSE International School - 223QMB", "distance": "7 mins (477 m)"}]',
'["BBQ", "Fitness Corner", "Parking", "Pool Deck", "Security", "Swimming Pool"]');

-- Record 3: 1 Canberra
INSERT INTO condo_basic (condo_name, developer_name, street_name, postal_code, latitude, longitude, tenure, total_units, district, mrt_nearby, has_swimming_pool, has_gym, has_tennis_court, has_security, has_parking, property_type, top_date, neighbourhood, num_floors, num_blocks, amenities_json, facilities_json)
VALUES ('1 Canberra', 'MCC Land (Singapore) Pte Ltd', '15 Canberra Drive', '768873', 1.437617, 103.829271, '99 YEARS', 665, 27, 'Canberra MRT · 7 min walk', TRUE, TRUE, TRUE, TRUE, TRUE, 'Executive Condo', '19 Sep 2015', 'Yishun', 13, 13,
'[{"name": "Ahmad Ibrahim Secondary School", "distance": "3 mins (152 m)"}]',
'["BBQ", "Fitness Corner", "Parking", "Swimming Pool", "Tennis Court", "Security"]');

-- Show count
SELECT COUNT(*) as condo_count FROM condo_basic;
