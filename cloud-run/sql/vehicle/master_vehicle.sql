/*
Name: Master Vehicle Table

Author: "Jon Dabbs"
Contributors: ""
Created: "2023-02-15"
Version: "0.2"
    Version:
        0.1 : 2023-02-15 - Updated to reflect discussed changes to create vehicle master table - JD
        0.2 : 2023-03-22 - Exclusion of GetirNoMaintenance from productfamily, uptime, islive VM - JD

Inputs:
# `central-dev-f7c3.fleetio.vehicle_assignment_history`
# `central-excellence-dev-9954.fleetio_clean.work_order_meta`
# `central-dev-f7c3.fleetio_vehicle.vehicle`
# `central-dev-f7c3.sheets.locations`
# `central-dev-f7c3.sheets.b2b_customer_information_scheduled`


Outputs:
# `central-ops-datamart-4fe3.master_vehicle.master_vehicle` (view)

Description:
# This master vehicle table has been compiled from stakeholder input across the business and is the central source of truth for vehicle data.
*/
  
WITH
  vehicle_assignments AS(
  SELECT
    vehicle_id,
    COUNT(contact_id) AS historical_operator_count
  FROM
    `central-dev-f7c3.fleetio.vehicle_assignment_history`
  GROUP BY
    1 ),
  work_order_meta AS(
  SELECT
    zid,
    vid,
    MAX(completed_at_utc) AS latest_completed_wo
  FROM
    `central-excellence-dev-9954.fleetio_clean.work_order_meta` wo
  GROUP BY
    1,
    2 ),
  raw_vehicle AS (
  SELECT
    v.name AS zid,
    v.id AS vehicle_id
    -- Location Data
    ,
    v.group_name AS fleetio_group,
    v.group_id AS fleetio_group_id,
    COALESCE(l.group_ancestry, 'Unknown') AS fleetio_group_ancestry,
    COALESCE(l.site_owner, 'Unknown') AS site_owner,
    COALESCE(l.site, 'Unknown') AS site,
    COALESCE(l.city, 'Unknown') AS city,
    COALESCE(l.city_short, 'Unknown') AS city_short,
    COALESCE(l.state, 'Unknown') AS state,
    COALESCE(l.state_short, 'Unknown') AS state_short,
    COALESCE(l.supply_region, 'Unknown') AS supply_region,
    COALESCE(l.supply_region_short, 'Unknown') AS supply_region_short,
    l.is_launched AS city_is_launched,
    COALESCE(l.country, 'Unknown') AS country,
    COALESCE(l.country_short, 'Unknown') AS country_short,
    COALESCE(l.region, 'Unknown') AS region,
    COALESCE(l.region_short, 'Unknown') AS region_short,
    COALESCE(l.mega_region, 'Unknown') AS mega_region,
    COALESCE(l.mega_region_short, 'Unknown') AS mega_region_short,
    l.maintenance_type AS site_maintenance_type,
    l.timezone,
    CASE
      WHEN l.maintenance_type = 'Onsite' THEN CONCAT(l.country_short, ' - ', l.site_owner, ' - ', l.site)
      WHEN l.maintenance_type = 'Remote' THEN CONCAT(l.country_short, ' - ', l.site_owner, ' - Remote')
    ELSE
    CONCAT(l.country_short, ' - Other Sites')
  END
    AS location,
    v.original_location,
    CASE
      WHEN LEFT(vv.contact_full_name, 5) = '[B2B]' THEN 'B2B'
      WHEN vv.contact_full_name IS NOT NULL THEN 'B2C'
    ELSE
    'Unassigned'
  END
    AS b2x,
    CASE
      WHEN LEFT(vv.contact_full_name, 5) = '[B2B]' THEN b.Segment
      WHEN vv.contact_full_name IS NOT NULL
    AND vv.current = TRUE THEN 'B2C'
    ELSE
    NULL
  END
    AS b2x_segment,
    CASE
      WHEN LEFT(vv.contact_full_name, 5) = '[B2B]' THEN SPLIT(vv.contact_full_name, ' | ')[SAFE_OFFSET(2)]
      WHEN vv.contact_full_name IS NULL THEN NULL
    ELSE
    vv.contact_full_name
  END
    AS customer,
    CASE
      WHEN v.type_name IN ('B2B|Rent', 'B2B|RTO', 'B2B|Buffer-Paid') OR (vv.contact_full_name LIKE '%[B2B]%' AND (v.type_name <> 'B2B|Buffer-Unpaid' AND (v.type_name LIKE '%1-rent%' OR v.type_name LIKE '%2-rto%' OR v.type_name LIKE '%Buffer%'))) THEN 'Rental'
      WHEN ((vv.contact_full_name IN ('[B2B] | US | Getir',
          '[B2B] | UK | Getir',
          '[B2B] | AU | MilkRun',
          '[B2B] | UK | Zapp',
          '[B2B] | US | Doordash')
        AND (v.type_name LIKE '%3-buy%'
          OR v.type_name LIKE '%2-own%')))
    OR v.type_name IN ('B2B|Buy-Maintenance',
      'B2B|Maintenance') THEN 'Maintenance Contracts'
      WHEN (((vv.contact_full_name IN ('[B2B] | US | Cornucopia', '[B2B] | UK | Upway') AND (v.type_name LIKE '%3-buy%' OR v.type_name LIKE '%2-own%'))) OR v.type_name='B2B|Buy') AND historical_operator_count > 1 AND vv.contact_full_name <> '[B2B] | UK | GetirNoMaintenance' AND current_date <= DATE_ADD(CAST(vv.created_at AS date), INTERVAL 1 YEAR) THEN 'Used Outright Sale'
      WHEN (((vv.contact_full_name IN ('[B2B] | US | Cornucopia',
            '[B2B] | UK | Upway')
          AND (v.type_name LIKE '%3-buy%'
            OR v.type_name LIKE '%2-own%')))
      OR v.type_name='B2B|Buy')
    AND (historical_operator_count = 1
      OR historical_operator_count IS NULL)
    AND vv.contact_full_name <> '[B2B] | UK | GetirNoMaintenance'
    AND current_date <= DATE_ADD(CAST(vv.created_at AS date), INTERVAL 1 YEAR) THEN 'New Outright Sale'
      WHEN (((vv.contact_full_name IN ('[B2B] | US | Cornucopia', '[B2B] | UK | Upway') AND (v.type_name LIKE '%3-buy%' OR v.type_name LIKE '%2-own%'))) OR v.type_name='B2B|Buy') AND current_date > DATE_ADD(CAST(vv.created_at AS date), INTERVAL 1 YEAR) THEN 'Expired Outright Sale'
      WHEN v.type_name = 'B2B|Buffer-Unpaid' THEN 'Unpaid Buffer'
      WHEN v.type_name = 'Default' THEN 'Not Allocated'
    ELSE
    'B2C'
  END
    AS product_family,
    CASE
      WHEN v.type_name = 'B2B|Rent' THEN 'Rent'
      WHEN v.type_name = 'B2B|RTO' THEN 'RTO'
      WHEN v.type_name = 'B2B|Buffer-Paid' THEN 'Buffer'
      WHEN v.type_name = 'B2B|Buy' THEN 'Sale'
      WHEN v.type_name = 'B2B|Buy-Maintenance' THEN 'Sale & Maintenance Contract'
      WHEN v.type_name = 'B2B|Maintenance' THEN 'Maintenance Only'
    ELSE
    'Not Allocated'
  END
    AS product,
    CASE
      WHEN v.make = 'Vmoto' THEN 'Moped'
      WHEN v.make IN ('EAV',
      'Fulpra',
      'Urban Arrow') THEN 'Cargo Bike'
    ELSE
    'Bike'
  END
    AS vehicle_sku,
    CASE
      WHEN v.make IN ('Vmoto', 'EAV', 'Fulpra', 'Urban Arrow', 'Zoomo', 'ZERO') THEN CONCAT(v.make, " ", v.model)
    ELSE
    'Third Party Bike'
  END
    AS make_model,
    CONCAT(v.make, " ", v.model) AS make_model_detail,
    CASE
      WHEN l.site = 'Investigation Group' THEN TRUE
    ELSE
    FALSE
  END
    AS is_in_investigation,
    CASE
      WHEN v.vehicle_status_name IN ('S0. Staging', 'S2. On Inventory', 'S3. Assembled', 'S4. Quality Assured') THEN 'Boxed Bikes'
      WHEN v.vehicle_status_name IN ('S5. Assembly Maintenance',
      'S6. Freight Issue',
      'S8. In Transfer',
      'S11. Maintenance',
      'S22. Maintenance Customer',
      'S24. Awaiting Pickup',
      'S24. Awaiting Pick-up',
      'S31. Needs Maintenance - B2B',
      'S33. Long Term Maintenance',
      'S23. External Maintenance',
      'S39. Asset Quarantine') THEN 'In Maintenance'
      WHEN v.vehicle_status_name IN ('S9. Fleet Available', 'S13. Reserved', 'S15. Sales (New)', 'S17. For Delivery', 'S32. Fleet Available - B2B') THEN 'Available Fleet'
      WHEN v.vehicle_status_name IN ('S10. Active',
      'S12. Internal Use',
      'S30. Works Well - B2B') THEN 'Active Fleet'
      WHEN v.vehicle_status_name IN ('S34. Awaiting Decommission', 'S40. Overdue Investigation') THEN 'Pending Write Off'
      WHEN v.vehicle_status_name IN ('S7. DOA',
      'S19. RMA',
      'S19. Warranty Inquiry',
      'S20. Part Redistribution',
      'S21. Decommissioned') THEN 'Write Off Vehicles'
      WHEN v.vehicle_status_name = 'S14. In Recovery' THEN 'In Recovery'
      WHEN v.vehicle_status_name = 'S1. In Transit' THEN 'Incoming Fleet'
      WHEN v.vehicle_status_name = 'S16. Sales (Used)' THEN 'Sales Allocation'
      WHEN v.vehicle_status_name = 'S18. Asset Sold' THEN 'Sold Vehicles'
    ELSE
    NULL
  END
    AS fleet_group,
    CASE
      WHEN l.site = 'Investigation Group' THEN 'Bikes in Investigation'
      WHEN v.vehicle_status_name IN ('S0. Staging',
      'S2. On Inventory',
      'S3. Assembled',
      'S4. Quality Assured',
      'S5. Assembly Maintenance',
      'S6. Freight Issue',
      'S8. In Transfer',
      'S11. Maintenance',
      'S22. Maintenance Customer',
      'S24. Awaiting Pickup',
      'S24. Awaiting Pick-up',
      'S31. Needs Maintenance - B2B',
      'S33. Long Term Maintenance',
      'S23. External Maintenance',
      'S39. Asset Quarantine',
      'S1. In Transit',
      'S9. Fleet Available',
      'S13. Reserved',
      'S15. Sales (New)',
      'S16. Sales (Used)',
      'S17. For Delivery',
      'S32. Fleet Available - B2B',
      'S16. Sales (Used)',
      'S34. Awaiting Decommission') THEN 'Stock (Inventory)'
      WHEN v.vehicle_status_name IN ('S10. Active', 'S12. Internal Use', 'S30. Works Well - B2B') THEN 'Leased Asset'
    ELSE
    NULL
  END
    AS fleet_group_finance
    -- for rob yang to confirm: null comprises: S14. In Recovery; S18. Asset Sold; S7. DOA'; S19. RMA; S19. Warranty Inquiry;
    -- S20. Part Redistribution S21. Decommissioned
    ,
    v.vehicle_status_name AS fleetio_status,
    v.type_name AS fleetio_type,
    v.archived_at AS archived_at_utc,
    CASE
      WHEN v.archived_at IS NOT NULL THEN TRUE
    ELSE
    FALSE
  END
    AS is_archived,
    NULLIF(v.vin, '') AS vin,
    NULLIF(v.supplier_serial_number,'') AS frame_serial_number,
    NULLIF(v.iot_device_type, '') AS iot_device_type,
    NULLIF(v.iot_serial_number, '') AS iot_serial_number,
    vv.current AS fleetio_current_assignment_status,
    vv.created_at AS vehicle_assignment_created_at_utc,
    vv.started_at AS vehicle_assignment_started_at_utc,
    vv.ended_at AS vehicle_assignment_ended_at_utc,
    v.created_at AS vehicle_created_at_utc,
    CASE
      WHEN vv.current = TRUE THEN vv.contact_full_name
      WHEN vv.current = FALSE THEN 'Unassigned'
    ELSE
    NULL
  END
    AS fleetio_operator,
    vv.contact_id,
    historical_operator_count,
    assembly_date,
    CASE
      WHEN assembly_date IS NULL OR assembly_date > CURRENT_DATE() THEN NULL
    ELSE
    DATE_DIFF(CURRENT_DATE(), SAFE_CAST(assembly_date AS DATE),DAY)
  END
    AS vehicle_age_days,
    DATE_DIFF(CURRENT_DATE(),CAST(latest_completed_wo AS DATE),DAY) AS days_since_last_service
  FROM
    `central-dev-f7c3.fleetio_vehicle.vehicle` v
  LEFT JOIN
    `central-dev-f7c3.fleetio.vehicle_assignment` vv
  ON
    v.id = vv.vehicle_id
  LEFT JOIN
    vehicle_assignments va
  ON
    v.id = va.vehicle_id
  LEFT JOIN
    work_order_meta wo
  ON
    v.id = wo.vid
  LEFT JOIN
    `central-dev-f7c3.sheets.locations` l
  ON
    v.group_id = l.group_id
  LEFT JOIN
    `central-dev-f7c3.sheets.b2b_customer_information_scheduled` b
  ON
    LOWER(SPLIT(vv.contact_full_name, ' | ')[SAFE_OFFSET(2)]) = LOWER(b.Customer)
    AND l.country_short = b.Country
  WHERE
    v.name <> 'Z123456'
    AND LOWER(v.name) NOT LIKE '%test%'
    AND LOWER(v.name) NOT LIKE '%shell%' )
SELECT
  a.*
  -- Boolean Fields using raw_vehicle data
  ,
  CASE
    WHEN fleet_group IN ('Active Fleet') AND fleetio_type NOT IN ('B2B|Buffer-Paid', 'B2B|Buffer-Unpaid') AND customer <> 'GetirNoMaintenance' AND LEFT(zid, 3) <> 'ACJ' THEN TRUE
  ELSE
  FALSE
END
  AS customer_uptime_numerator,
  CASE
    WHEN fleet_group IN ('Active Fleet', 'In Recovery', 'In Maintenance') AND fleetio_type NOT IN ('B2B|Buffer-Paid', 'B2B|Buffer-Unpaid') AND customer <> 'GetirNoMaintenance' AND LEFT(zid, 3) <> 'ACJ' THEN TRUE
  ELSE
  FALSE
END
  AS customer_uptime_denominator,
  CASE
    WHEN fleetio_status IN ('S8. In Transfer', 'S9. Fleet Available', 'S10. Active', 'S12. Internal Use', 'S13. Reserved', 'S15. Sales (New)', 'S16. Sales (Used)', 'S17. For Delivery', 'S24. Awaiting Pick-up', 'S30. Works Well - B2B', 'S32. Fleet Available - B2B') AND customer <> 'GetirNoMaintenance' AND LEFT(zid, 3) <> 'ACJ' THEN TRUE
  ELSE
  FALSE
END
  AS true_uptime_numerator,
  CASE
    WHEN fleetio_status IN ('S8. In Transfer', 'S9. Fleet Available', 'S10. Active', 'S11. Maintenance', 'S12. Internal Use', 'S13. Reserved', 'S14. In Recovery', 'S15. Sales (New)', 'S16. Sales (Used)', 'S17. For Delivery', 'S24. Awaiting Pick-up', 'S30. Works Well - B2B', 'S31. Needs Maintenance - B2B', 'S32. Fleet Available - B2B') AND customer <> 'GetirNoMaintenance' AND LEFT(zid, 3) <> 'ACJ' THEN TRUE
  ELSE
  FALSE
END
  AS true_uptime_denominator,
  CASE
    WHEN fleet_group = 'Boxed Bikes' OR fleet_group = 'Available Fleet' AND historical_operator_count IS NULL THEN TRUE
  ELSE
  FALSE
END
  AS is_new_vehicle,
  CASE
    WHEN fleetio_current_assignment_status = TRUE AND fleet_group NOT IN ('Write Off Vehicles', 'Sold Vehicles') AND customer <> 'GetirNoMaintenance' THEN TRUE
  ELSE
  FALSE
END
  AS is_live,
  CASE
    WHEN fleet_group NOT IN ('Write Off Vehicles', 'Sold Vehicles') THEN TRUE
  ELSE
  FALSE
END
  AS is_current_fleet,
  CASE
    WHEN b2x = 'B2B' AND fleetio_current_assignment_status = TRUE AND product_family IN ('Rental', 'Maintenance Contracts', 'Used Outright Sale', 'New Outright Sale') AND fleet_group NOT IN ('Write Off Vehicles', 'Pending Write Off') THEN TRUE
    WHEN fleetio_current_assignment_status = TRUE
  AND b2x <> 'B2B' THEN NULL
  ELSE
  FALSE
END
  AS is_vehicles_moved_b2b -- to be sense checked vs VM
FROM
  raw_vehicle a