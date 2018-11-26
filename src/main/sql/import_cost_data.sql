open schema PUB1901;

-- Import the CSV file
--
IMPORT INTO Costs_Staging
FROM LOCAL SECURE CSV
FILE '/path/to/import/file.csv'
(1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 21, 24, 25, 129, 130, 131, 132)
ENCODING = 'ASCII'
SKIP = 1
COLUMN SEPARATOR = ',';

-- Turn staged data into production data
--
INSERT INTO Costs
SELECT
    id,
    period,
    invoice,
    payerAccount,
    TO_DATE(ISO_TS_TO_DATE(billingStart)),
    TO_DATE(ISO_TS_TO_DATE(billingEnd)),
    usageAccount,
    costType,
    TO_DATE(ISO_TS_TO_DATE(usageStart)),
    TO_DATE(ISO_TS_TO_DATE(usageEnd)),
    awsProduct,
    operation,
    az,
    resource,
    currency,
    blendedRate,
    blendedCost,
    createdBy,
    department,
    owner,
    project
FROM Costs_Staging;

-- Empty the stage as preparation for the next import run
--
TRUNCATE TABLE Costs_Staging;