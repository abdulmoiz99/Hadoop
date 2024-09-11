import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;

public class OccurenceOfEachLetter extends Configured implements Tool {

    public static class OccurenceOfEachLetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      
       Text word = new Text();
       
       HashMap<Character, Integer> characterMap;
       
       @Override
       protected void setup(Context context) throws IOException, InterruptedException {    
    	   characterMap = new HashMap<Character, Integer>();
    	   for(char i = 'A'; i<= 'Z'; i++){
    		   characterMap.put(i, 0);
    	   }
       }
      
       @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] wordsInLine = value.toString().toUpperCase().split(" ");
           
           for(String words: wordsInLine){
        	   characterMap.put(words.charAt(0), characterMap.get(words.charAt(0)) + 1);
           }
        }
       @Override
       protected void cleanup(Context context) throws IOException, InterruptedException {
    	   for(Map.Entry<Character, Integer> map: characterMap.entrySet()){
        	   word.set(map.getKey() + "");
        	   context.write(word, new IntWritable(map.getValue()));
    		}
       }
    }
    public static class OccurenceOfEachLetterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
        	int total = 0;
        	for(IntWritable value: values){
        		total+= Integer.parseInt(value.toString());
        	}
        	context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new OccurenceOfEachLetter(), args);
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

        Job job = Job.getInstance(getConf(), "OccurenceOfEachLetter");
        job.setJarByClass(OccurenceOfEachLetter.class);
        job.setMapperClass(OccurenceOfEachLetterMapper.class);
        job.setReducerClass(OccurenceOfEachLetterReducer.class);
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
