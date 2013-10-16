SELECT abs(- n), ~n,
  mod(n, 3), sqrt(n),
  char_length(s), octet_length(s),
  left(s, 3), right(s, 4),
  upper(s), lower(s),
  locate(s, 'abc'), locate(s, 'xyz', 10),
  substring(s, 10), substring(s, 10, 3),
  ltrim(s), trim(trailing '!' from s),
  s||s collate en_us_ci,
  month(d), timestamp(d, '13:01:01'), timestampdiff(day, d, current_timestamp),
  next value for seq, current value for seq
FROM t
  