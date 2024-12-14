




-----------------------------------------
----------------AGGREGATE YR TO DATE AVGS
SELECT s.Source
	,avg([RelHumidity(%)]) [RelHumidity(%)]
	,rank() OVER (
		ORDER BY avg([RelHumidity(%)])
		) [RelHumidity(%)Rank]
	,avg([AirTemperature(degC)]) [AirTemperature(degC)]
	,rank() OVER (
		ORDER BY avg([AirTemperature(degC)])
		) [AirTemperature(degC)Rank]
	,avg([GlobalHoriz(W/m^2)]) [GlobalHoriz(W/m^2)]
	,rank() OVER (
		ORDER BY avg([GlobalHoriz(W/m^2)])
		) [GlobalHoriz(W/m^2)Rank]
	,avg([DailyTempFluctuation(degC)]) [DailyTempFluctuation(degC)]
	,rank() OVER (
		ORDER BY avg([DailyTempFluctuation(degC)])
		) [DailyTempFluctuation(degC)Rank]
	,avg([StationPressure(mBar)]) [StationPressure(mBar)]
	,rank() OVER (
		ORDER BY avg([StationPressure(mBar)])
		) [StationPressure(mBar)Rank]
FROM (
	SELECT Source
	FROM master.dbo.slr_envrnmnt
	) s
LEFT JOIN (
	SELECT Source
		,avg([RelHumidity(%)]) [RelHumidity(%)]
	FROM master.dbo.slr_envrnmnt
	WHERE [RelHumidity(%)] >= 0
	GROUP BY Source
	) a ON a.source = s.source
LEFT JOIN (
	SELECT Source
		,avg([AirTemperature(degC)]) [AirTemperature(degC)]
	FROM master.dbo.slr_envrnmnt
	WHERE [AirTemperature(degC)] > - 50
	GROUP BY Source
	) b ON b.Source = s.Source
LEFT JOIN (
	SELECT Source
		,avg([GlobalHoriz(W/m^2)]) [GlobalHoriz(W/m^2)]
	FROM (
		SELECT Source
			,DATETIME
			,CASE 
				WHEN Source = 'UFL'
					AND cast(DATETIME AS DATE) BETWEEN '2024-01-01'
						AND '2024-01-31'
					THEN NULL
				ELSE [GlobalHoriz(W/m^2)]
				END AS [GlobalHoriz(W/m^2)]
		FROM master.dbo.slr_envrnmnt
		) i
	WHERE [GlobalHoriz(W/m^2)] BETWEEN - 10 AND 1500
	GROUP BY Source
	) c ON c.Source = s.Source
LEFT JOIN (
	SELECT Source
		,avg([DailyTempFluctuation(degC)]) [DailyTempFluctuation(degC)]
	FROM (
		SELECT Source
			,cast(DATETIME AS DATE) DATE
			,(max([AirTemperature(degC)]) - min([AirTemperature(degC)])) [DailyTempFluctuation(degC)]
		FROM master.dbo.slr_envrnmnt
		WHERE [AirTemperature(degC)] > - 50
		GROUP BY Source
			,cast(DATETIME AS DATE)
		) g
	GROUP BY Source
	) d ON d.Source = s.Source
LEFT JOIN (
	SELECT Source
		,avg([StationPressure(mBar)]) [StationPressure(mBar)]
	FROM master.dbo.slr_envrnmnt
	WHERE [StationPressure(mBar)] > - 500
	GROUP BY Source
	) e ON e.Source = s.Source
GROUP BY s.source



-----------------------------------------------
----------------AGGREGATE YR TO DATE DAILY AVGS
SELECT s.Source
	,avg([RelHumidity(%)]) [RelHumidity(%)]
	,rank() OVER (
		ORDER BY avg([RelHumidity(%)])
		) [RelHumidity(%)Rank]
	,avg([AirTemperature(degC)]) [AirTemperature(degC)]
	,rank() OVER (
		ORDER BY avg([AirTemperature(degC)])
		) [AirTemperature(degC)Rank]
	,avg([GlobalHoriz(W/m^2)]) [GlobalHoriz(W/m^2)]
	,rank() OVER (
		ORDER BY avg([GlobalHoriz(W/m^2)])
		) [GlobalHoriz(W/m^2)Rank]
	,avg([DailyTempFluctuation(degC)]) [DailyTempFluctuation(degC)]
	,rank() OVER (
		ORDER BY avg([DailyTempFluctuation(degC)])
		) [DailyTempFluctuation(degC)Rank]
	,avg([StationPressure(mBar)]) [StationPressure(mBar)]
	,rank() OVER (
		ORDER BY avg([StationPressure(mBar)])
		) [StationPressure(mBar)Rank]
FROM (
	SELECT Source
		,cast(DATETIME AS DATE) DATE
	FROM master.dbo.slr_envrnmnt
	GROUP BY source
		,cast(DATETIME AS DATE)
	) s
LEFT JOIN (
	SELECT Source
		,cast(DATETIME AS DATE) DATE
		,avg([RelHumidity(%)]) [RelHumidity(%)]
	FROM master.dbo.slr_envrnmnt
	WHERE [RelHumidity(%)] >= 0
	GROUP BY Source
		,cast(DATETIME AS DATE)
	) a ON a.source = s.source
	AND a.DATE = s.DATE
LEFT JOIN (
	SELECT Source
		,cast(DATETIME AS DATE) DATE
		,avg([AirTemperature(degC)]) [AirTemperature(degC)]
	FROM master.dbo.slr_envrnmnt
	WHERE [AirTemperature(degC)] > - 50
	GROUP BY Source
		,cast(DATETIME AS DATE)
	) b ON b.Source = s.Source
	AND b.DATE = s.DATE
LEFT JOIN (
	SELECT Source
		,cast(DATETIME AS DATE) DATE
		,avg([GlobalHoriz(W/m^2)]) [GlobalHoriz(W/m^2)]
	FROM (
		SELECT Source
			,DATETIME
			,CASE 
				WHEN Source = 'UFL'
					AND cast(DATETIME AS DATE) BETWEEN '2024-01-01'
						AND '2024-01-31'
					THEN NULL
				ELSE [GlobalHoriz(W/m^2)]
				END AS [GlobalHoriz(W/m^2)]
		FROM master.dbo.slr_envrnmnt
		) i
	WHERE [GlobalHoriz(W/m^2)] BETWEEN - 10 AND 1500
	GROUP BY Source
		,cast(DATETIME AS DATE)
	) c ON c.Source = s.Source
	AND C.DATE = s.DATE
LEFT JOIN (
	SELECT Source
		,avg([DailyTempFluctuation(degC)]) [DailyTempFluctuation(degC)]
		,DATE
	FROM (
		SELECT Source
			,cast(DATETIME AS DATE) DATE
			,(max([AirTemperature(degC)]) - min([AirTemperature(degC)])) [DailyTempFluctuation(degC)]
		FROM master.dbo.slr_envrnmnt
		WHERE [AirTemperature(degC)] > - 50
		GROUP BY Source
			,cast(DATETIME AS DATE)
		) g
	GROUP BY Source
		,DATE
	) d ON d.Source = s.Source
	AND d.DATE = s.DATE
LEFT JOIN (
	SELECT Source
		,cast(DATETIME AS DATE) DATE
		,avg([StationPressure(mBar)]) [StationPressure(mBar)]
	FROM master.dbo.slr_envrnmnt
	WHERE [StationPressure(mBar)] > - 500
	GROUP BY Source
		,cast(DATETIME AS DATE)
	) e ON e.Source = s.Source
	AND e.DATE = s.DATE
GROUP BY s.source