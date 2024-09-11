users = LOAD '/home/cloudera/Desktop/SampleData/MovieDataSet/users.txt' USING PigStorage('|') AS (id:int, age:int, gender:chararray, role:chararray, counter:int);
filtered_users = FILTER users BY (gender == 'M' and role == 'lawyer');
filtered_lawyers = FOREACH filtered_users GENERATE role, gender;

grouped_data = GROUP filtered_lawyers BY role;

cnt = FOREACH grouped_data GENERATE group, COUNT(filtered_lawyers);

dump cnt;
--9|29|M|student|01002