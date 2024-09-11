products = LOAD '/home/cloudera/Desktop/SampleData/product.txt' USING PigStorage(',') AS (p_id:chararray, name:chararray, price:double);
filtered = FILTER products BY price > 50.0;
filtered_products = FOREACH filtered GENERATE p_id;


orders = LOAD '/home/cloudera/Desktop/SampleData/orderDetails.txt' USING PigStorage(',') AS (id:int, p_id:chararray, date:chararray);

product_orders  = JOIN filtered_products BY p_id, orders BY p_id;

group_data = GROUP product_orders BY filtered_products::p_id;

cnt = FOREACH group_data GENERATE group, COUNT(product_orders);

dump cnt;