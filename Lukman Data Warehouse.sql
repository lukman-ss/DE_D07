-- Nama: Lukman

-- 1 A. Dimension table "dim_user"
CREATE TABLE dim_user AS
SELECT
    u.id AS u_id,
    u.c_id,
    u.email,
    u.first_name || ' ' || u.last_name AS full_name,
    u.gender,
    u.DOB,
    TO_CHAR(u.register_date, 'YYYY-MM-DD') AS register_date,
    CASE
        WHEN ia.id IS NOT NULL THEN 'Instagram'
        WHEN fa.id IS NOT NULL THEN 'Facebook'
        ELSE 'unknown'
    END AS ads_source,
    DATE_PART('year', CURRENT_DATE) - DATE_PART('year', u.DOB) AS age
FROM dibimbing.user.users u
LEFT JOIN dibimbing.social_media.facebook_ads fa ON u.c_id = fa.id
LEFT JOIN dibimbing.social_media.instagram_ads ia ON u.c_id = ia.id;

-- 1 b. Dimension table "dim_ads"
CREATE TABLE dim_ads AS
SELECT
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM social_media.facebook_ads

UNION ALL

SELECT
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM social_media.instagram_ads;

-- 2a. "fact_user_performance"
CREATE TABLE fact_user_performance AS
SELECT
    u.id AS u_id,
    u.first_name || ' ' || u.last_name AS user_name,
    MAX(e.timestamp)::date AS last_login,
    MAX(e.timestamp)::date AS last_activity,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.id END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount,
    AVG(CASE WHEN e.event_type = 'search' THEN (e.event_data->>'total_result')::integer ELSE 0 END) AS avg_search_results
FROM dibimbing.user.users u
LEFT JOIN dibimbing.event."User Event" e ON u.id = e.u_id
LEFT JOIN dibimbing.user.user_transactions t ON u.id = t.u_id
GROUP BY u.id;

-- 2b. fact_ads_performance
CREATE TABLE fact_ads_performance AS
SELECT
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN a.id END) AS total_converted,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM (
    SELECT id, ads_id, 'Facebook' AS ads_source FROM social_media.facebook_ads
    UNION ALL
    SELECT id, ads_id, 'Instagram' AS ads_source FROM social_media.instagram_ads
) AS a
LEFT JOIN dibimbing.user.users AS u ON a.id = u.c_id
LEFT JOIN dibimbing.event."User Event" AS e ON a.ads_id = e.event_data->>'ads_id'
LEFT JOIN dibimbing.user.user_transactions AS t ON u.id = t.u_id AND t.transaction_type = 'purchase'
GROUP BY a.ads_id;

-- 3a. CREATE TABLE fact_daily_event_performance 
CREATE TABLE fact_daily_event_performance AS
SELECT
    e.timestamp::date AS event_date,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'logout' THEN e.id END) AS total_logouts,
    COUNT(DISTINCT u.id) AS total_users,
    COUNT(DISTINCT CASE WHEN t.transaction_type = 'purchase' THEN u.id END) AS total_purchasing_users,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM dibimbing.event."User Event" e
LEFT JOIN dibimbing.user.users u ON e.u_id = u.id
LEFT JOIN dibimbing.user.user_transactions t ON u.id = t.u_id
GROUP BY event_date
ORDER BY event_date;

-- 3b. CREATE TABLE fact_weekly_ads_performance 
CREATE TABLE fact_weekly_ads_performance AS
SELECT
    DATE_TRUNC('week', a.timestamp) AS week_start,
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN a.id END) AS total_converted_users,
FROM (
    SELECT id, ads_id, 'Facebook' AS ads_source, timestamp FROM social_media.facebook_ads
    UNION ALL
    SELECT id, ads_id, 'Instagram' AS ads_source, timestamp FROM social_media.instagram_ads
) AS a
LEFT JOIN dibimbing.user.users AS u ON a.id = u.c_id
LEFT JOIN dibimbing.user.user_transactions AS t ON u.id = t.u_id AND t.transaction_type = 'purchase'
GROUP BY week_start, a.ads_id;
