package com.cloudera.sa.mapreduce.sleepjob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SleepJob 
{
	public static final String MAPPER_SLEEP_TIME = "mapperSleepTime";
	public static final String REDUCER_SLEEP_TIME = "reducerSleepTime";
	
    public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
    {
        if (args.length != 5) {
        	System.out.println("SleepJob <numberOfMappers> <numberOfRecuers> <mapperSleepTime> <reducerSleepTime> <tmpOutdir>");
        	return;
        }
        
        int numberOfMappers = Integer.parseInt(args[0]);
        int numberOfReducers = Integer.parseInt(args[1]);
        int mapperSleepTime = Integer.parseInt(args[2]);
        int reducerSleepTime = Integer.parseInt(args[3]);
        String tmpOutputDirectory = args[4];
        
        // Create job
        Job job = new Job();

        job.getConfiguration().set(MAPPER_SLEEP_TIME, Integer.toString(mapperSleepTime));
        job.getConfiguration().set(REDUCER_SLEEP_TIME, Integer.toString(reducerSleepTime));
        
        job.setJarByClass(SleepJob.class);
        // Define input format and path
        job.setInputFormatClass(SelectNumberOfMappersInputFormat.class);
        SelectNumberOfMappersInputFormat.setMapperNumber(job, numberOfMappers);

        // Define output format and path
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(tmpOutputDirectory));

        // Define the mapper and reducer
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        // Define the key and value format
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(numberOfReducers);

        // Exit
        job.waitForCompletion(true);
        
        Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		hdfs.delete(new Path(tmpOutputDirectory), true);
    }
    
    public static class CustomMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
    	int sleepTime = 0;
    	
    	@Override
    	public void setup(Context context) {
    		sleepTime = Integer.parseInt(context.getConfiguration().get(MAPPER_SLEEP_TIME));
    	}
    	
    	@Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    		System.out.println("Sleeping " + sleepTime + " at " + System.currentTimeMillis());
    		Thread.sleep(sleepTime);
    		System.out.println("Done Sleeping at " + System.currentTimeMillis());
    	}
    }
    
    public static class CustomReducer extends Reducer<Text, Text, NullWritable, Text> {
    	
    	int sleepTime = 0;
    	
    	@Override
    	public void setup(Context context) throws InterruptedException {
    		sleepTime = Integer.parseInt(context.getConfiguration().get(REDUCER_SLEEP_TIME));
    		System.out.println("Sleeping " + sleepTime + " at " + System.currentTimeMillis());
    		Thread.sleep(sleepTime);
    		System.out.println("Done Sleeping at " + System.currentTimeMillis());
    	}
    	
    	@Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    		
    	}
    }
}
