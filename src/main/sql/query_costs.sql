open schema PUB1901;

-- Costs by project
SELECT billingStart, billingEnd, ROUND(SUM(blendedCost), 2), currency, project
FROM Costs
GROUP BY billingStart, billingEnd, project, currency
ORDER BY SUM(blendedCost) DESC;

-- Costs by owner
SELECT billingStart, billingEnd, ROUND(SUM(blendedCost), 2), currency, owner
FROM Costs
GROUP BY billingStart, billingEnd, owner, currency
ORDER BY SUM(blendedCost) DESC;

-- Costs by owner / "created by"
SELECT billingStart, billingEnd, ROUND(SUM(blendedCost), 2), currency, OWNER_FALLBACK(owner, createdBy)
FROM Costs
GROUP BY billingStart, billingEnd, OWNER_FALLBACK(owner, createdBy), currency
ORDER BY SUM(blendedCost) DESC;

-- Tagged resource ownership
SELECT department, project, owner, awsProduct, resource
FROM Costs
GROUP BY awsProduct, resource, department, project, owner
ORDER BY department, project, owner, awsProduct, resource;

-- Wrongly tagged resources
SELECT OWNER_FALLBACK(owner, createdBy), awsProduct, resource
FROM Costs
WHERE department IS NULL OR project IS NULL OR OWNER IS NULL
GROUP BY OWNER_FALLBACK(owner, createdBy), awsProduct, resource
ORDER BY OWNER_FALLBACK(owner, createdBy), awsProduct, resource;