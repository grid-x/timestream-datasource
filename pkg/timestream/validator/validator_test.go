package validator

import "testing"

func TestValidate_MoreCases(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		desc  string
		input string
		want  bool
	}{
		{
			desc: "direct select with >= ago()",
			input: `
SELECT *
FROM mydb.sensors
WHERE time >= ago(15m) AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "direct select missing WHERE completely",
			input: `
SELECT *
FROM mydb.sensors`,
			want: false,
		},
		{
			desc: "WHERE present but no time predicate",
			input: `
SELECT *
FROM mydb.sensors
WHERE measure_name = 'cpu'`,
			want: false,
		},
		{
			desc: "BETWEEN with now()",
			input: `
SELECT *
FROM mydb.sensors
WHERE time BETWEEN ago(1d) AND now() AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "aggregation with GROUP BY and time filter",
			input: `
SELECT measure_name, avg(measure_value::double) AS v
FROM mydb.sensors
WHERE time > ago(1h) AND measure_name = 'foo'
GROUP BY measure_name`,
			want: true,
		},
		{
			desc: "JOIN with time filter in WHERE",
			input: `
SELECT *
FROM mydb.s1
JOIN mydb.s2 ON s1.device = s2.device
WHERE time >= ago(2h)`,
			want: false,
		},
		{
			desc: "JOIN without time filter",
			input: `
SELECT *
FROM mydb.s1
JOIN mydb.s2 ON s1.device = s2.device
WHERE s1.device <> ''`,
			want: false,
		},
		{
			desc: "CTEs (both sources time-filtered)",
			input: `
WITH a AS (
  SELECT * FROM mydb.s1
  WHERE time >= ago(1h) AND measure_name = 'foo'
),
b AS (
  SELECT * FROM mydb.s2
  WHERE time > ago(2h) AND measure_name = 'bar'
)
SELECT *
FROM a
JOIN b ON a.device = b.device`,
			want: true,
		},
		{
			desc: "CTEs (one source missing time filter)",
			input: `
WITH a AS (
  SELECT * FROM mydb.s1
),
b AS (
  SELECT * FROM mydb.s2
  WHERE time > ago(2h)
)
SELECT *
FROM a
JOIN b ON a.device = b.device`,
			want: false,
		},
		{
			desc: "derived table with inner time filter",
			input: `
SELECT x.*
FROM (
  SELECT *
  FROM mydb.s1
  WHERE time >= ago(5m)
	AND measure_name = 'foo'
) x
WHERE x.measure_value::double > 0`,
			want: true,
		},
		{
			desc: "derived table missing inner time filter",
			input: `
SELECT x.*
FROM (
  SELECT *
  FROM mydb.s1
) x`,
			want: false,
		},
		{
			desc: "UNION ALL with both sides filtered",
			input: `
SELECT *
FROM mydb.s1
WHERE measure_name = 'foo' AND time >= ago(1h)
UNION ALL
SELECT *
FROM mydb.s2
WHERE measure_name = 'foo' AND time >= ago(1h)`,
			want: true,
		},
		{
			desc: "UNION ALL with one side missing time filter",
			input: `
SELECT *
FROM mydb.s1
WHERE time >= ago(1h)
UNION ALL
SELECT *
FROM mydb.s2`,
			want: false,
		},
		{
			desc: "SELECT literal only (no FROM) is ignored",
			input: `
SELECT 1`,
			want: true,
		},
		{
			desc: "commented out time predicate should not count",
			input: `
SELECT *
FROM mydb.s1
WHERE /* time >= ago(1h) */ measure_name = 'x'`,
			want: false,
		},
		{
			desc: "time comparison with function",
			input: `
SELECT *
FROM mydb.s1
WHERE time >= from_iso8601_timestamp('2025-01-01T00:00:00Z') AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "NOT BETWEEN on time",
			input: `
SELECT *
FROM mydb.s1
WHERE NOT time BETWEEN ago(1h) AND now() AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "nested CTEs with inner-filtered source",
			input: `
WITH a AS (
  SELECT * FROM mydb.s1 WHERE time >= ago(1h) AND measure_name = 'foo'
),
z AS (
  WITH inner AS (
    SELECT * FROM mydb.s3 WHERE time >= ago(2h) AND measure_name = 'foo'
  )
  SELECT * FROM inner
)
SELECT * FROM a`,
			want: true,
		},
		{
			desc: "quoted db/table, unquoted time",
			input: `
SELECT *
FROM "mydb"."s1"
WHERE time >= ago(10m) AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "time predicate placed in HAVING (invalid per rules)",
			input: `
SELECT device, max(time) AS t
FROM mydb.s1
GROUP BY device
HAVING max(time) >= ago(1h)`,
			want: false,
		},
		{
			desc:  "Free /data for devices",
			input: `SELECT   device AS "Device",   MIN(measure_value::double/1024/1024) AS "Free /data [MB]"  FROM   "ds-metric-forward"."metrics" WHERE   time BETWEEN from_milliseconds(1755664656155) AND from_milliseconds(1755668256155)   AND measure_value::double < 1024000000   AND measure_name = 'gridx.ds.system.storage./data.available' GROUP BY   device ORDER BY   device`,
			want:  true,
		},
		{
			desc:  "Free /data for devices (timefilter missing)",
			input: `SELECT   device AS "Device",   MIN(measure_value::double/1024/1024) AS "Free /data [MB]"  FROM   "ds-metric-forward"."metrics" WHERE  AND measure_value::double < 1024000000   AND measure_name = 'gridx.ds.system.storage./data.available' GROUP BY   device ORDER BY   device`,
			want:  false,
		},
		{
			desc: "integrations availability indicator",
			input: `WITH q AS (
  SELECT 
    BIN(time, 60s) AS binned_timestamp,
    -- Calculate the percentage of online appliances for each time bin.
    -- COALESCE and NULLIF safely handle cases with zero total appliances, preventing division-by-zero errors.
    COALESCE(
      (
        CAST(COUNT(DISTINCT CASE WHEN measure_value::double > 0 THEN appliance END) AS double)
      ) / NULLIF(COUNT(DISTINCT appliance), 0),
      0
    ) AS percentage_online
  FROM "ds-metric-forward"."metrics"
  WHERE
		time BETWEEN from_milliseconds(1755693908186) AND from_milliseconds(1755697508186) AND
    measure_name = 'gridx.monitoring.appliance_online' AND
    releasegroup IN ('stable','canary','alpha')
  GROUP by BIN(time, 60s)
  ORDER by BIN(time, 60s)
)
SELECT 
  MAX(percentage_online) as online
FROM q
`,
			want: true,
		},
		{
			desc: "integrations availability indicator",
			input: `WITH q AS (
  SELECT 
    BIN(time, 60s) AS binned_timestamp,
    -- Calculate the percentage of online appliances for each time bin.
    -- COALESCE and NULLIF safely handle cases with zero total appliances, preventing division-by-zero errors.
    COALESCE(
      (
        CAST(COUNT(DISTINCT CASE WHEN measure_value::double > 0 THEN appliance END) AS double)
      ) / NULLIF(COUNT(DISTINCT appliance), 0),
      0
    ) AS percentage_online
  FROM "ds-metric-forward"."metrics"
  WHERE
    measure_name = 'gridx.monitoring.appliance_online' AND
    releasegroup IN ('stable','canary','alpha')
  GROUP by BIN(time, 60s)
  ORDER by BIN(time, 60s)
)
SELECT 
  MAX(percentage_online) as online
FROM q
`,
			want: false,
		},
		{
			desc: "BETWEEN with now(), no measure_name",
			input: `
SELECT *
FROM mydb.sensors
WHERE time BETWEEN ago(1d) AND now()`,
			want: false,
		},
		{
			desc: "measure_name inequality",
			input: `
SELECT *
FROM mydb.sensors
WHERE time BETWEEN ago(1d) AND now() AND measure_name != 'bar'`,
			want: false,
		},
		{
			desc: "measure_name in parantheses",
			input: `
SELECT *
FROM mydb.sensors
WHERE time BETWEEN ago(1d) AND now() AND (measure_name = 'foo')`,
			want: true,
		},
		{
			desc: "time in parantheses",
			input: `
SELECT *
FROM mydb.sensors
WHERE (time BETWEEN ago(1d) AND now()) AND measure_name = 'foo'`,
			want: true,
		},
		{
			desc: "invalid measure_name (mixed equality and inequality)",
			input: `SELECT * FROM "db"."tbl"
                    WHERE time > 10
                    AND measure_name = 'foo'
                    AND measure_name != 'bar'`,
			want: false, // Fails due to measure_name check
		},
		{
			desc: "user query with top-level OR that bypasses filter",
			input: `SELECT DISTINCT ds_account FROM "db"."tbl" WHERE
                    time BETWEEN from_milliseconds(1) AND from_milliseconds(2)
                    AND measure_name = 'foo'
                    AND ds_account != 'provisioning'
                    OR ds_account != 'eis'`,
			want: false,
		},
		{
			desc: "user query with nested OR",
			input: `SELECT DISTINCT ds_account FROM "db"."tbl" WHERE
                    time BETWEEN from_milliseconds(1) AND from_milliseconds(2)
                    AND measure_name = 'foo'
                    AND (ds_account != 'provisioning'
                    OR ds_account != 'eis')`,
			want: true,
		},
		{
			desc: "valid top-level OR with filters in each branch",
			input: `SELECT * FROM "db"."tbl"
                    WHERE (time > 10 AND measure_name = 'a')
                    OR (time < 5 AND measure_name = 'b')`,
			want: true,
		},
		{
			desc: "valid parenthesized OR with top-level filters",
			input: `SELECT * FROM "db"."tbl"
                    WHERE time > 10 AND measure_name = 'a'
                    AND (device = 'd1' OR device = 'd2')`,
			want: true,
		},
		{
			desc: "valid parenthesized OR with top-level filters",
			input: `SELECT * FROM "db"."tbl"
                    WHERE time > 10 AND measure_name = 'a'
                    AND (device = 'd1' OR device = 'd2')`,
			want: true,
		},
		{
			desc: "valid top-level OR with nested (parenthesized) ORs",
			input: `SELECT * FROM "db"."tbl"
                    WHERE (time > 10 AND measure_name = 'a' AND (device = 'd1' OR device = 'd2'))
                    OR (time < 5 AND measure_name = 'b' AND (device = 'd3' OR device = 'd4'))`,
			want: true,
		},
		{
			desc: "invalid top-level OR, one branch has nested OR but no time filter",
			input: `SELECT * FROM "db"."tbl"
                    WHERE (time > 10 AND measure_name = 'a')
                    OR (measure_name = 'b' AND (device = 'd1' OR device = 'd2'))`,
			want: false,
		},
		{
			desc: "FALSE POSITIVE: invalid top-level OR, one branch has nested OR but no time filter",
			input: `SELECT * FROM "db"."tbl"
					WHERE
  					(time > ago(1h) OR device = 'd1')
  					AND measure_name = 'foo'`,
			want: true, // This is a false positive as the current implementation only checks for OR clauses at the Top-Level
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			got, issues := Validate(tc.input, nil)
			if got != tc.want {
				t.Errorf("%s: want %v, got %v, issues: %+v", tc.desc, tc.want, got, issues)
			}
		})
	}
}
