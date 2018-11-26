CREATE SCHEMA AwsCostControl; -- Change name if your IT created one for you 'e.g. PUB****'

-- This Lua script extracts the date part of an ISO date/timestamp so that it can be fed to the `TO_CHAR()` function.
--
-- A typical example of such a timestamp is: 2018-11-22T11:52:39Z
--
CREATE OR REPLACE LUA SCALAR SCRIPT ISO_TS_TO_DATE(iso VARCHAR(20)) RETURNS VARCHAR(10) AS
function run(context)
    if context.iso == null then
        return '0001-01-01'
    else
        return string.sub(context.iso, 1, 10)
    end
end
/

-- This Lua script either returns the owner of a resource (if the tag was present in AWS) or falls back to creation
-- metadata AWS provides for some of the resources. This in some cases allows mapping resources to owners who did not
-- tag the properly.
--
CREATE OR REPLACE LUA SCALAR SCRIPT OWNER_FALLBACK(owner VARCHAR(200), createdBy VARCHAR(2000)) RETURNS VARCHAR(200) AS
function run(context)
    if context.owner ~= null then
        return context.owner
    else
        if context.createdBy == null then
            return 'unknown'
        else
            return string.gsub(context.createdBy, 'IAM.*:', '')
        end
    end
end
/

-- A staging table allows us to first import the data from the CSV as-is using Exasol's `IMPORT` statement. This is
-- necessary because the conversion of ISO timestamps to DATE values is not supported out-of-the-box.
--
DROP TABLE IF EXISTS Costs_Staging;
CREATE TABLE Costs_Staging (
    id VARCHAR(100),                  -- A/1:    lineItem/LineItemId
    period VARCHAR(41),               -- B/2:    identity/TimeInterval
    invoice VARCHAR(200),             -- C/3:    bill/InvoiceId
    payerAccount DECIMAL(18,0),       -- F/6:    bill/PayerAccountId
    billingStart VARCHAR(20),         -- G/7:    bill/BillingPeriodStartDate
    billingEnd VARCHAR(20),           -- H/8:    bill/BillingPeriodEndDate
    usageAccount DECIMAL(18,0),       -- I/9:    lineItem/UsageAccountId
    costType VARCHAR(20),             -- J/10:   lineItem/LineItemType ("e.g. 'Tax' or 'Usage')
    usageStart VARCHAR(20),           -- K/11:   lineItem/UsageStartDate
    usageEnd VARCHAR(20),             -- L/12:   lineItem/UsageEndDate
    awsProduct VARCHAR(200),          -- M/13:   lineItem/ProductCode
    operation VARCHAR(200),           -- O/15:   lineItem/Operation
    az VARCHAR(20),                   -- P/16:   lineItem/AvailabilityZone    
    resource VARCHAR(2000),           -- Q/17:   lineItem/ResourceId
    currency VARCHAR(20),             -- U/21:   lineItem/CurrencyCode
    blendedRate DECIMAL(18,10),       -- X/24:   lineItem/BlendedRate
    blendedCost DECIMAL(18,10),       -- Y/25:   lineItem/BlendedCost
    createdBy VARCHAR(2000),          -- DY/:129 resourceTags/aws:createdBy
    department VARCHAR(200),          -- DZ/:130 resourceTags/exa:department
    owner VARCHAR(200),               -- EA/:131 resourceTags/exa:owner
    project VARCHAR(200)              -- EB/:132 resourceTags/exa:project
);

-- This table contains the cost data after format conversions that are done in the step from staging to production data.
--
DROP TABLE IF EXISTS Costs;
CREATE TABLE Costs (
    id VARCHAR(100),                  -- A/1:    lineItem/LineItemId
    period VARCHAR(41),               -- B/2:    identity/TimeInterval
    invoice VARCHAR(200),             -- C/3:    bill/InvoiceId
    payerAccount DECIMAL(18,0),       -- F/6:    bill/PayerAccountId
    billingStart DATE,                -- G/7:    bill/BillingPeriodStartDate
    billingEnd DATE,                  -- H/8:    bill/BillingPeriodEndDate
    usageAccount DECIMAL(18,0),       -- I/9:    lineItem/UsageAccountId
    costType VARCHAR(20),             -- J/10:   lineItem/LineItemType ("e.g. 'Tax' or 'Usage')
    usageStart DATE,                  -- K/11:   lineItem/UsageStartDate
    usageEnd DATE,                    -- L/12:   lineItem/UsageEndDate
    awsProduct VARCHAR(200),          -- M/13:   lineItem/ProductCode
    operation VARCHAR(200),           -- O/15:   lineItem/Operation
    az VARCHAR(20),                   -- P/16:   lineItem/AvailabilityZone    
    resource VARCHAR(2000),           -- Q/17:   lineItem/ResourceId
    currency VARCHAR(20),             -- U/21:   lineItem/CurrencyCode
    blendedRate DECIMAL(18,10),       -- X/24:   lineItem/BlendedRate
    blendedCost DECIMAL(18,10),       -- Y/25:   lineItem/BlendedCost
    createdBy VARCHAR(2000),          -- DY/:129 resourceTags/aws:createdBy
    department VARCHAR(200),          -- DZ/:130 resourceTags/exa:department
    owner VARCHAR(200),               -- EA/:131 resourceTags/exa:owner
    project VARCHAR(200)              -- EB/:132 resourceTags/exa:project
);

-- Create an entry in this table whenever you import a new CSV file in order to be able to trace if and when cost data
-- was imported.
--
DROP TABLE IF EXISTS Imports;
CREATE TABLE Imports (
    sessionId INTEGER,
    ts TIMESTAMP,
    message VARCHAR(2000)
);