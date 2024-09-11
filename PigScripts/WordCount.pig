dataset = LOAD '/home/cloudera/Desktop/SampleData/InputForWC.txt' USING TextLoader AS (line: chararray);
word_token = FOREACH dataset GENERATE TOKENIZE(line, '\t ') AS words;
word_flat = FOREACH word_token GENERATE FLATTEN(words) AS word;
grouped_words = GROUP word_flat BY word;
cnt = FOREACH grouped_words GENERATE  group, COUNT(word_flat);
dump cnt;