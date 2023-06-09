import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); //type of output key
       
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] mydata = value.toString().split(" ");
            for (String data:mydata) {
            	word.set(data); //set word as each input keyword
            	context.write(word, one); // create a pair <keyword, 1>
            	
            	// this is UTD
            	// this, 1
            	// is , 1
            	// is , 1
            	// UTD, 1
            }
        }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable(0);
        
      
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        	// is , [1,1]
        	// this, [1]
        	
            int sum = 0; //initialize the sum for each keyword
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result); //create a pair <keyword, number of occurences>
        }
        
        // is, 2
        // this, 1
	}
	
	//Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=2) {
        	System.err.println(otherArgs.length);
        	System.err.println("Usage: WordCount <in> <out>");
        	System.exit(2);
        }
        
        //create a job with name "wordcount"
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
