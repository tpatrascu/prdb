SELECT key, value, SUM(value) FROM records GROUP BY key, value HAVING (SUM(value) >= 5 OR SUM(value) = $2) AND NOT SUM(value) < 20 ORDER BY key LIMIT 4
