import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Homework1Q2 {
	public static class MapperClass extends Mapper <LongWritable, Text, Text, IntWritable>
	{	
		private HashMap<String, String> userdata = new HashMap<>();
		
		protected void setup(Context context) throws IOException, InterruptedException {			
			
			Configuration conf = context.getConfiguration();
			Path pt = new Path (context.getConfiguration().get("data")); // get file path to userdata.txt
			FileSystem fs = FileSystem.get(conf); 
			
			// read the file
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] data = line.split(",");
				userdata.put(data[0],  data[9]); // save userID and DOB to hashmap
			}
			
		}
		
		protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] users = values.toString().split("\t");
			
			if (users.length == 2) {
				String user = users[0]; // get userID
				
				String[] friends = users[1].split(","); // friends of user
				
				// floop through friends of user
				for (String friend: friends) {
					try {
						int friendAge = getAge(userdata.get(friend)); // calculate each friend's age
						
						context.write(new Text(user), new IntWritable(friendAge)); // emit userID and age of friend
						
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}
		}
		
		// calculates the number of years passed from birthdate to 01.01.2023
		private int getAge(String birthdate) throws ParseException {
			
			// Convert birthdate to one format
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");  
			Date parsed  = formatter.parse(birthdate);
			
			// Convert Date object to LocalDate object
			LocalDate bday = parsed.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			
			// Current Date
			LocalDate now = LocalDate.of(2023,  1, 1);
			
			// Calculate period between dates in years
			int age = Period.between(bday, now).getYears();
			
			return age;
		}
	}
	
    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    	    	
    	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		
    		int total = 0;
    		int count = 0;
    		
    		// accumulate total count of ages
    		for (IntWritable ages: values) {
    			total += ages.get();
    			count++; // keep track of number of ages
    		}
    		
    		int averageAge = total / count; // calculate average age
    		
    		// emit userID and age of average friends.
    		context.write(key, new IntWritable(averageAge));
    	}
    	

	}


    public static void main(String[] args) throws Exception{
    	Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	
		if(otherArgs.length!=3) {
			System.out.println("Usage: AverageAge <mutual friends data path> <user data path> <output path>");
			System.exit(1);
		}
		
		
		conf.set("data", otherArgs[1]);
		Job job = Job.getInstance(conf, "Homework1Q2");
		

		job.setJarByClass(Homework1Q2.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}

	
}
