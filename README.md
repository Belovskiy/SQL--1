WITH sessions AS (
  SELECT
    user_id,
    event,
    event_time,
    value,
    SUM(CASE WHEN DATEDIFF(MINUTE, LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time), event_time) > 5 THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM
    logs
)
SELECT
  value AS template_name,
  COUNT(*) AS consecutive_uses
FROM
  (
    SELECT
      user_id,
      session_id,
      value,
      COUNT(*) OVER (PARTITION BY user_id, session_id, grp) AS consecutive_uses
    FROM
      (
        SELECT
          user_id,
          session_id,
          value,
          SUM(CASE WHEN LAG(event) OVER (PARTITION BY user_id, session_id ORDER BY event_time) = 'template_selected' AND event = 'template_selected' THEN 0 ELSE 1 END) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS grp
        FROM
          sessions
        WHERE
          event = 'template_selected'
      ) t
  ) t
WHERE
  consecutive_uses >= 2
GROUP BY
  value
ORDER BY
  consecutive_uses DESC
LIMIT
  5;
