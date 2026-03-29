"""
ClickBench queries — 43 canonical queries for both ClickHouse and PostgreSQL.
Differences handled:
  Q19  : toMinute()        → EXTRACT(MINUTE FROM ...)
  Q29  : replaceRegexpOne  → REGEXP_REPLACE
  Q43  : toStartOfMinute() → DATE_TRUNC('minute', ...)
"""

_SUM90 = ", ".join(f"sum(ResolutionWidth + {i})" for i in range(90))

QUERY_LABELS = [
    "Total row count",
    "Count where AdvEngineID ≠ 0",
    "Sum AdvEngineID, count, avg ResolutionWidth",
    "Average UserID",
    "Count distinct UserID",
    "Count distinct SearchPhrase",
    "Min/max EventDate",
    "Top AdvEngineID by count",
    "Top regions by unique users",
    "Region rollup: sum, count, avg, distinct users",
    "Top mobile models by unique users",
    "Top mobile phone + model by unique users",
    "Top search phrases by count",
    "Top search phrases by unique users",
    "Top search engine + phrase by count",
    "Top users by event count",
    "Top user + phrase by count",
    "User + phrase count (no ORDER BY)",
    "User + minute + phrase by count",
    "Lookup single UserID",
    "Count rows with 'google' in URL",
    "Top search phrases from google URLs",
    "Top phrases: Google in title, not google.* in URL",
    "Full rows from google URLs (LIMIT 10)",
    "Search phrases ordered by time",
    "Search phrases ordered by phrase",
    "Search phrases ordered by time then phrase",
    "Counters by avg URL length (>100k hits)",
    "Referer domain by avg length (>100k hits)",
    "90× sum of ResolutionWidth offsets",
    "Search engine + IP rollup (search traffic)",
    "WatchID + IP rollup (search traffic)",
    "WatchID + IP rollup (all traffic)",
    "Top URLs by view count",
    "Top URLs by view count (with literal 1)",
    "ClientIP arithmetic group-by",
    "Counter 62: top URLs Jul 2013 (no bounce)",
    "Counter 62: top titles Jul 2013 (no bounce)",
    "Counter 62: linked non-download URLs (offset 1000)",
    "Counter 62: traffic source × URL (offset 1000)",
    "Counter 62: by URLHash + referer hash (offset 100)",
    "Counter 62: by window size + URLHash (offset 10000)",
    "Counter 62: page views per minute (offset 1000)",
]

CLICKHOUSE_QUERIES = [
    # Q1
    "SELECT count(*) FROM hits",
    # Q2
    "SELECT count(*) FROM hits WHERE AdvEngineID <> 0",
    # Q3
    "SELECT sum(AdvEngineID), count(*), avg(ResolutionWidth) FROM hits",
    # Q4
    "SELECT avg(UserID) FROM hits",
    # Q5
    "SELECT count(DISTINCT UserID) FROM hits",
    # Q6
    "SELECT count(DISTINCT SearchPhrase) FROM hits",
    # Q7
    "SELECT min(EventDate), max(EventDate) FROM hits",
    # Q8
    "SELECT AdvEngineID, count(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY count(*) DESC LIMIT 10",
    # Q9
    "SELECT RegionID, count(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    # Q10
    "SELECT RegionID, sum(AdvEngineID), count(*) AS c, avg(ResolutionWidth), count(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    # Q11
    "SELECT MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q12
    "SELECT MobilePhone, MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q13
    "SELECT SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q14
    "SELECT SearchPhrase, count(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    # Q15
    "SELECT SearchEngineID, SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q16
    "SELECT UserID, count(*) FROM hits GROUP BY UserID ORDER BY count(*) DESC LIMIT 10",
    # Q17
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q18
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    # Q19 — ClickHouse-specific toMinute()
    "SELECT UserID, toMinute(EventTime) AS m, SearchPhrase, count(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q20
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    # Q21
    "SELECT count(*) FROM hits WHERE URL LIKE '%google%'",
    # Q22
    "SELECT SearchPhrase, min(URL), count(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q23
    "SELECT SearchPhrase, min(URL), min(Title), count(*) AS c, count(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q24
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    # Q25
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    # Q26
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    # Q27
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    # Q28
    "SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q29 — ClickHouse-specific replaceRegexpOne
    r"SELECT replaceRegexpOne(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, avg(length(Referer)) AS l, count(*) AS c, min(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q30
    f"SELECT {_SUM90} FROM hits",
    # Q31
    "SELECT SearchEngineID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q32
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q33
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q34
    "SELECT URL, count(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    # Q35
    "SELECT 1, URL, count(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    # Q36
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    # Q37
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    # Q38
    "SELECT Title, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    # Q39
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q40
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q41
    "SELECT URLHash, EventDate, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    # Q42
    "SELECT WindowClientWidth, WindowClientHeight, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    # Q43 — ClickHouse-specific toStartOfMinute()
    "SELECT toStartOfMinute(EventTime) AS M, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY toStartOfMinute(EventTime) ORDER BY toStartOfMinute(EventTime) LIMIT 10 OFFSET 1000",
]

POSTGRES_QUERIES = [
    # Q1
    "SELECT count(*) FROM hits",
    # Q2
    "SELECT count(*) FROM hits WHERE AdvEngineID <> 0",
    # Q3
    "SELECT sum(AdvEngineID), count(*), avg(ResolutionWidth) FROM hits",
    # Q4
    "SELECT avg(UserID) FROM hits",
    # Q5
    "SELECT count(DISTINCT UserID) FROM hits",
    # Q6
    "SELECT count(DISTINCT SearchPhrase) FROM hits",
    # Q7
    "SELECT min(EventDate), max(EventDate) FROM hits",
    # Q8
    "SELECT AdvEngineID, count(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY count(*) DESC LIMIT 10",
    # Q9
    "SELECT RegionID, count(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    # Q10
    "SELECT RegionID, sum(AdvEngineID), count(*) AS c, avg(ResolutionWidth), count(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    # Q11
    "SELECT MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q12
    "SELECT MobilePhone, MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    # Q13
    "SELECT SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q14
    "SELECT SearchPhrase, count(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    # Q15
    "SELECT SearchEngineID, SearchPhrase, count(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q16
    "SELECT UserID, count(*) FROM hits GROUP BY UserID ORDER BY count(*) DESC LIMIT 10",
    # Q17
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q18
    "SELECT UserID, SearchPhrase, count(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    # Q19 — PostgreSQL EXTRACT instead of toMinute()
    "SELECT UserID, EXTRACT(MINUTE FROM EventTime)::integer AS m, SearchPhrase, count(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY count(*) DESC LIMIT 10",
    # Q20
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    # Q21
    "SELECT count(*) FROM hits WHERE URL LIKE '%google%'",
    # Q22
    "SELECT SearchPhrase, min(URL), count(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q23
    "SELECT SearchPhrase, min(URL), min(Title), count(*) AS c, count(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    # Q24
    "SELECT WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, ClientIP, RegionID, UserID, OS, UserAgent, URL, Referer, IsRefresh, ResolutionWidth, ResolutionHeight, MobilePhoneModel, SearchPhrase, AdvEngineID, WindowClientWidth, WindowClientHeight, PageCharset, IsLink, IsDownload, IsNotBounce, URLHash FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
    # Q25
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    # Q26
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    # Q27
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    # Q28
    "SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q29 — PostgreSQL REGEXP_REPLACE instead of replaceRegexpOne
    r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, avg(length(Referer)) AS l, count(*) AS c, min(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25",
    # Q30
    f"SELECT {_SUM90} FROM hits",
    # Q31
    "SELECT SearchEngineID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q32
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q33
    "SELECT WatchID, ClientIP, count(*) AS c, sum(IsRefresh), avg(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
    # Q34
    "SELECT URL, count(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    # Q35
    "SELECT 1, URL, count(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    # Q36
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
    # Q37
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    # Q38
    "SELECT Title, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    # Q39
    "SELECT URL, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q40
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
    # Q41
    "SELECT URLHash, EventDate, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
    # Q42
    "SELECT WindowClientWidth, WindowClientHeight, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000",
    # Q43 — PostgreSQL DATE_TRUNC instead of toStartOfMinute()
    "SELECT DATE_TRUNC('minute', EventTime) AS M, count(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000",
]

assert len(CLICKHOUSE_QUERIES) == 43, f"Expected 43 CH queries, got {len(CLICKHOUSE_QUERIES)}"
assert len(POSTGRES_QUERIES) == 43, f"Expected 43 PG queries, got {len(POSTGRES_QUERIES)}"
assert len(QUERY_LABELS) == 43, f"Expected 43 labels, got {len(QUERY_LABELS)}"
