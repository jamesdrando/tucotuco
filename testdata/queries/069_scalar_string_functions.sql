SELECT
  UPPER('tucotuco') AS upper_text,
  LOWER('TUCOTUCO') AS lower_text,
  TRIM(FROM '  tucotuco  ') AS trimmed_text,
  TRIM(LEADING '.' FROM '..tucotuco') AS left_trimmed,
  TRIM(TRAILING '.' FROM 'tucotuco..') AS right_trimmed,
  SUBSTRING('tucotuco' FROM 2 FOR 4) AS slice_text,
  POSITION('tu' IN 'tucotuco') AS first_pos,
  CHAR_LENGTH('tucotuco') AS char_len,
  CHARACTER_LENGTH('tucotuco') AS character_len,
  OCTET_LENGTH('tucotuco') AS octet_len,
  OVERLAY('abcdef' PLACING 'ZZ' FROM 2 FOR 3) AS overlay_text,
  CONCAT('sql', '') AS concat_text;
SELECT
  REGEXP_LIKE('abc123', '[0-9]+') AS has_digits,
  REGEXP_REPLACE('abc123', '[0-9]+', 'x') AS stripped_text,
  REGEXP_SUBSTR('abc123', '[0-9]+') AS matched_digits;
