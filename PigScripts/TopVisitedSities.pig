users = LOAD '/home/cloudera/Desktop/SampleData/users.csv' USING PigStorage(',') AS (id:int, name:chararray, age:int);
filtered_users = FILTER users BY ( age >= 18 AND age <= 25);

pages = LOAD '/home/cloudera/Desktop/SampleData/pages.csv' USING PigStorage(',') AS (id:int, page:chararray, user_id:int);

user_pages = JOIN users BY id, pages BY user_id;

visits = FOREACH user_pages GENERATE users::name AS username, pages::page AS page;

grouped_visits = GROUP visits BY page;

visits_count = FOREACH grouped_visits GENERATE group, COUNT(visits) AS page_count;

order_data = ORDER visits_count BY page_count DESC;

top_5 = LIMIT order_data 5;

dump top_5;
