SELECT key, value, COUNT(*) FROM records GROUP BY key, value HAVING NOT (COUNT(*) < 4 OR COUNT(*) = $1)
