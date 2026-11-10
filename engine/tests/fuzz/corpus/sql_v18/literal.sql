SELECT key, value, SUM(value) FROM records GROUP BY key, value HAVING SUM(value) >= 9223372036854775807
