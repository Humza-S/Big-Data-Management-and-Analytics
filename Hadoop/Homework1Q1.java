import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Homework1Q1 {
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
    	
    	private static final Text[] OUTPUT_KEY_PAIRS_VALUES = 
    		{ new Text("0,1"), new Text("20,28193"), new Text("1,29826"), new Text("6222,19272"), new Text("28041,28056")};
    	
    	// required output pairs
    	private static final LinkedHashSet<Text> OUTPUT_KEY_PAIRS_SET = new LinkedHashSet<>(Arrays.asList(OUTPUT_KEY_PAIRS_VALUES)); 
    	    	
    	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashSet<String> mutuals  = new HashSet<>(); // keep track of mutual friends
			
			for (Text tuples: values) {
				String[] friendsList = tuples.toString().split(","); // gets friends list for each friend
				HashSet<String> friendsSet = new HashSet<>(Arrays.asList(friendsList));
				
				if (mutuals.isEmpty()) // when mutual is initially empty we will add all friends of the first friend
					mutuals.addAll(friendsSet);
				else
					mutuals.retainAll(friendsSet); // keeps all common friends
			}
			
			// if there are no mutual friends we output None and if there are mutual friends we create a new Text of mutual friends separated my commas
			Text mutualsOutput = (mutuals.isEmpty() ? new Text("No Mutual Friends") : new Text(String.join(",", mutuals))); 
			
			// emit only desired keys
			if (OUTPUT_KEY_PAIRS_SET.contains(key))
				context.write(key, new Text(mutualsOutput));
    	}
	}


    public static void main(String[] args) throws Exception{
		if(args.length!=2) {
			System.out.println("Usage: MutualFriends <mutual friends data path> <output path>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Homework1Q1");

		job.setJarByClass(Homework1Q1.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}
	}

	
}
