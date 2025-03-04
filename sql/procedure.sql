CREATE PROCEDURE UpdateDailySearchVolumes()
BEGIN
	INSERT INTO keyword_search_volume_daily (keyword_id, created_date, anchor_datetime, search_volume)
	SELECT
		tmp.keyword_id,
		tmp.data_date as created_date,
		tmp.anchor_time as anchor_datetime,
		tmp.search_volume
	FROM
		(
		SELECT
			keyword_id,
			DATE(created_datetime) AS data_date,
			search_volume,
			created_datetime AS anchor_time,
			ABS(TIME_TO_SEC(TIMEDIFF(created_datetime, DATE_ADD(DATE(created_datetime), INTERVAL '09:00:00' HOUR_SECOND)))) AS time_difference,
			ROW_NUMBER() OVER (
		        PARTITION BY keyword_id,
				DATE(created_datetime)
				ORDER BY ABS(TIME_TO_SEC(TIMEDIFF(created_datetime, DATE_ADD(DATE(created_datetime), INTERVAL '09:00:00' HOUR_SECOND))))
			) AS row_rank
		FROM keyword_search_volume
		) tmp
	WHERE tmp.row_rank = 1
	ON DUPLICATE KEY UPDATE search_volume = tmp.search_volume;
END