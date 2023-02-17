SELECT * FROM operational.`sql project-1 table_csv`;
show databases;
use operational;
show tables;
alter table operational.`sql project-1 table_csv` rename job_data;
drop table operational.`sql project-1 table_csv`;


/* A. Number of jobs reviewed: Amount of jobs reviewed over time.
Your task: Calculate the number of jobs reviewed per hour 
per day for November 2020? */ 

Select ds, round(1.0*count(job_id)*3600/sum(time_spent),1)
as throughput
from job_data
where 
event in ('transfer','decision') and 
ds between '2020-11-01' and '2020-11-30'
GROUP BY ds;


/* B. Throughput: It is the no. of events happening per second.
Your task: Let’s say the above metric is called throughput.
Calculate 7 day rolling average of throughput? For throughput, 
do you prefer daily metric or 7-day rolling and why? */

with CTE AS (
SELECT 
DS,
COUNT(job_id) as num_jobs,
SUM(time_spent) as total_time
From job_data
Where event In ('transfer','decision')
And ds Between '2020-11-01' and '2020-11-30'
Group by ds)
Select ds,round(1.0*sum(num_jobs) over
(Order by ds rows between 6 Preceding and current row) / 
sum(total_time) OVER (order by ds rows between 6 Preceding and current row),2) as throughput_for_7days
From CTE;

/* C. Percentage share of each language: Share of each language for different contents.
Your task: Calculate the percentage share of each language in the last 30 days? */

WITH CTE AS (
SELECT Language,
COUNT(job_id) as num_jobs
From job_data
Where event In('transfer','decision')
And ds Between '2020-11-01' and '2020-11-30'
Group by language),
Total as(Select COUNT(job_id) as total_jobs
From job_data
Where event In('transfer','decision')
And ds Between '2020-11-01' and '2020-11-30'
Group by language)
Select distinct Language,
Round(100*num_jobs / total_jobs,2) as percentage_of_jobs
From CTE
cross join total
Order by percentage_of_jobs DESC;

/* D. Duplicate rows: Rows that have the same value present in them.
Your task: Let’s say you see some duplicate rows in the data. 
How will you display duplicates from the table?  */

select * from job_data;
select job_id,ds,actor_id, count(job_id) FROM JOB_DATA
group by job_id,ds,actor_id
having count(job_id)>1;


/* A. User Engagement: To measure the activeness of a user. 
Measuring if the user finds quality in a product/service.  */

show events;
drop table events;

show tables;
alter table operational.`table-2 events` rename events;

SELECT date_trunc('week',occurred_at),
count(Distinct user_id) as weekly_active_users
from events 
where event_type='engagement'
and event_name='login'
group by 1
order by 1;

/* B. User Growth: Amount of users growing over time for a product.
Your task: Calculate the user growth for product?  */

select date_trunc('day', created_at) as day, 
count(*) as all_users,
count(case when activated_at is
NOT NULL THEN user_id 
else NULL END)
as activated_users
from users
where created_at >= '2013-01-01'
and created_at < '2013-01-31'
group by 1
order by 1;


/* C. Weekly Retention: Users getting retained weekly after signing-up for a product.
Your task: Calculate the weekly retention of users-sign up cohort? */

select * from operational.`table-1 users`;
alter table operational.`table-1 users` rename users;
select * from users;

SELECT DATE_TRUNC('week',z.occurred_at) AS "week",
AVG(z.age_at_event) AS "Average age during week",
COUNT(DISTINCT CASE WHEN z.user_age > 70 THEN z.user_id ELSE NULL END)
AS "10+ weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 70 AND z.user_age >= 63 THEN
z.user_id ELSE NULL END) AS "9 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 63 AND z.user_age >= 56 THEN
z.user_id ELSE NULL END) AS "8 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 56 AND z.user_age >= 49 THEN
z.user_id ELSE NULL END) AS "7 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 49 AND z.user_age >= 42 THEN
z.user_id ELSE NULL END) AS "6 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 42 AND z.user_age >= 35 THEN
z.user_id ELSE NULL END) AS "5 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 35 AND z.user_age >= 28 THEN
z.user_id ELSE NULL END) AS "4 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 28 AND z.user_age >= 21 THEN
z.user_id ELSE NULL END) AS "3 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 21 AND z.user_age >= 14 THEN
z.user_id ELSE NULL END) AS "2 weeks",
COUNT(DISTINCT CASE WHEN z.user_age < 14 AND z.user_age >= 7 THEN
z.user_id ELSE NULL END) AS "1 week",
COUNT(DISTINCT CASE WHEN z.user_age < 7 THEN z.user_id ELSE NULL END) AS
"Less than a week"
FROM (
SELECT e.occurred_at,
u.user_id,
DATE_TRUNC('week',u.activated_at) AS activation_week,
EXTRACT('day' FROM e.occurred_at - u.activated_at) AS age_at_event,
EXTRACT('day' FROM '2014-09-01'::TIMESTAMP - u.activated_at) AS user_age
FROM users u
JOIN events e
ON e.user_id = u.user_id
AND e.event_type = 'engagement'
AND e.event_name = 'login'
AND e.occurred_at >= '2014-05-01'
AND e.occurred_at < '2014-09-01'
WHERE u.activated_at IS NOT NULL
) z
GROUP BY 1
ORDER BY 1

/* D. Weekly Engagement: To measure the activeness of a user. 
Measuring if the user finds quality in a product/service weekly. */

select DATE_TRUNC('week', occurred_at) as week,
COUNT(distinct e.user_id) as weekly_active_users,
count(distinct CASE WHEN e.device in('macbook pro','lenovo thinkpad','macbook air','dell inspiron 
notebook','hp pavilion desktop','accer aspire desktop','mac mini')
Then e.user_id else null end) as computer,
count(distinct case when e.device in('iphone 5','samsung galaxy s4','nexus 5','iphone 5',
'nokia lumia 635','htc one',
'samsung galaxy note','amzon fire phone') then e.user_id else null end) as phone,
count(distinct case when e.device in('ipad air','nexus 7','ipad mini','nexus 10','kindle fire','windows surface','samsung 
galaxy tablet')
then e.user_id else null end) as tablet
from events e
where e.event_type='engagement'
and e.event_name='login'
group by 1
order by 1
limit 100;

/* E. Email Engagement: Users engaging with the email service.
Your task: Calculate the email engagement metrics? */

select date_trunc('week', occurred_at) as week,
count(case when e.action='sent_weekly_digest' then e.user_id else null end) as weekly_emails,
count(case when e.action='sent_reengagement_email' then e.user_id else null end) as reengagement_email,
count(case when e.action='email_open' then e.user_id else null end) as email_opens,
count(case when e.action='email_clickthrough' then e.user_id else null end) as email_clickthroughs
from emails e
group by 1;

