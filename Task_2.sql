SELECT
  service_name, 
  COUNT(*) AS service_count
FROM services
GROUP BY service_name
ORDER BY service_count DESC
LIMIT 10;