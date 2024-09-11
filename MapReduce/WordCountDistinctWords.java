import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;

public class WordCountDistinctWords extends Configured implements Tool {

    public static class WordCountDistinctWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      
       Text word = new Text();
       IntWritable one = new IntWritable(1);
        
       @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] wordsInLine = value.toString().split(" ");
           for(String words: wordsInLine){
        	   word.set(words.toLowerCase());
        	   context.write(word, one);
           }
        }
    }

    public static class WordCountDistinctWordsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        	int count = 0;
        	for(IntWritable _: values){
        		count++;
        	}
        	if(count == 1)
        		context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new WordCountDistinctWords(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(getConf(), "WordCountDistinctWords");
        job.setJarByClass(WordCountDistinctWords.class);
        job.setMapperClass(WordCountDistinctWordsMapper.class);
        job.setReducerClass(WordCountDistinctWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
