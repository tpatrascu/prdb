SELECT key, value, SUM(value) FROM records GROUP BY key, value HAVING (SUM(value) > 1 OR NOT)
