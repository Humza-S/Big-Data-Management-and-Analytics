import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Homework1Q3 {
	public static class MapperClass extends Mapper <LongWritable, Text, Text, Text>
	{		
		protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] users = values.toString().split("\t");
			
			if (users.length == 2) {
				int user1ID = Integer.parseInt(users[0]);
				String[] usersList = users[1].split(",");
				int user2ID;
				Text tuple_key = new Text();
				
				for (String usr2 : usersList) {
					user2ID = Integer.parseInt(usr2);
					// defines the key where the first ID is smaller than the second ID
					if (user1ID < user2ID) {
						tuple_key.set(user1ID + "," + user2ID);
					} else {
						tuple_key.set(user2ID + "," + user1ID);
					}
					// emits the pair of IDs as the key and the list of their friends as the value
					context.write(tuple_key, new Text(users[1]));
				}
			}
		}
		
	}
	
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
    	
    	private HashMap<String, String> userdata = new HashMap<>();
    	private String user1;
    	private String user2;
    	    	
    	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		HashSet<String> mutuals  = new HashSet<>(); // keep track of mutual friends
    		
			for (Text tuples: values) {
				String[] friendsList = tuples.toString().split(","); // gets friends list for each friend
				LinkedHashSet<String> friendsSet = new LinkedHashSet<>(Arrays.asList(friendsList));
				
				if (mutuals.isEmpty()) // when mutual is initially empty we will add all friends of the first friend
					mutuals.addAll(friendsSet);
				else
					mutuals.retainAll(friendsSet); // keeps all common friends
			}
			
			// only continue if our key represents the specified users in the input
			if (key.toString().equals(new String(user1 + "," + user2))) {
				
				String[] dobs = new String[mutuals.size()]; // date of births of mutual friends of specified users
				
				
				int min_age = Integer.MAX_VALUE;
	
				
				int i = 0;
				
				// loop through mutual friends
				for (String friend: mutuals) {
					dobs[i] = userdata.get(friend); // get date of birth from hashmap
					try {
						int age = getAge(dobs[i]); // calculate age of mutual friend
						
						// determine if calculated age is the new minimum age
						if (age < min_age)
							min_age = age;
						
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					i++;
				}
			
				// outputs N/A if minimum age is maximum value (aka there were no mutual friends between the two specified users) and mimium age if there was a minimum age found
				String outputMinAge = (min_age == Integer.MAX_VALUE ? "N/A" : Integer.toString(min_age));
			
//				System.out.println(key.toString() + "\t" + Arrays.toString(dobs) + ", " + outputMinAge);
				
				context.write(key,  new Text(Arrays.toString(dobs) + ", " + outputMinAge));
			}
    	}
    	
    	// calculates the number of days passed from birthdate to 01.01.2023
    	private int getAge(String birthdate) throws ParseException {
			
			// Convert birthdate to one format
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");  
			Date parsed  = formatter.parse(birthdate);
			
			// Convert Date object to LocalDate object
			LocalDate bday = parsed.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

			// Current Date
			LocalDate now = LocalDate.of(2023,  1, 1);
			
			// Calculate days between dates
			int age = (int) ChronoUnit.DAYS.between(bday, now);
			
			return age;
		}
    	
    	protected void setup(Context context) throws IOException, InterruptedException {			
			
			Configuration conf = context.getConfiguration();
			Path pt = new Path (context.getConfiguration().get("data")); // get file path to userdata.txt
			FileSystem fs = FileSystem.get(conf);
			
			user1 = context.getConfiguration().get("user1"); // get id of user 1
			user2 = context.getConfiguration().get("user2"); // get id of user 2
			
			// read the file
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] data = line.split(",");
				userdata.put(data[0],  data[9]); // save userID and DOB
			}
		}
	}


    public static void main(String[] args) throws Exception{
    	
    	Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length!=5) {
			System.out.println("Usage: MutualFriends <mutual friends data path> <user data path> <user1 ID> <user2 ID> <output path>");
			System.exit(1);
		}
		
		
		conf.set("data", otherArgs[1]);
		conf.set("user1", otherArgs[2]);
		conf.set("user2", otherArgs[3]);
		
		Job job = Job.getInstance(conf, "Homework1Q3");

		job.setJarByClass(Homework1Q3.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}

	
}
