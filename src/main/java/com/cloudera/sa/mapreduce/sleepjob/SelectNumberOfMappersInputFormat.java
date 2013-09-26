package com.cloudera.sa.mapreduce.sleepjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SelectNumberOfMappersInputFormat  extends InputFormat<LongWritable, Text> {

	  private static final String MAPRED_MAP_TASKS = "mapred.map.tasks";

	  /**
	   * An input split consisting of a range on numbers.
	   */
	  static class ConfigurableInputSplit extends InputSplit implements Writable {

	    long firstRow = 0;

	    public ConfigurableInputSplit() {

	    }

	    public void write(DataOutput out) throws IOException {
	      WritableUtils.writeVLong(out, firstRow);

	    }

	    public void readFields(DataInput in) throws IOException {
	      firstRow = WritableUtils.readVLong(in);

	    }

	    public long getLength() throws IOException {
	      // TODO Auto-generated method stub
	      return 0;
	    }

	    public String[] getLocations() throws IOException {
	      return new String[] {};
	    }

	  }

	  static class ConfigurableRecordReader extends
	      RecordReader<LongWritable, Text> {

	    boolean isFirstRow = true;
	    
	    TaskAttemptContext context;
	    
	    boolean isSetup = false;
	    

	    public ConfigurableRecordReader() {
	    }

	    @Override
	    public void initialize(InputSplit split, TaskAttemptContext context)
	        throws IOException, InterruptedException {
	      
	      if (isSetup == false) {
	        isSetup = true;
	        
	        this.context = context; 
	        
	        System.out.println("Setup");

	        int numberOfMappers = context.getConfiguration().getInt(
	            MAPRED_MAP_TASKS, -1);

	        if (numberOfMappers == -1) {
	          throw new RuntimeException();
	        }
	      } 
	    }

	    @Override
	    public boolean nextKeyValue() throws IOException, InterruptedException {
	    
	      if (isFirstRow) {
	        isFirstRow = false;
	        return true;
	      } 
	      return false;
	    }

	    LongWritable newKey = new LongWritable();
	    
	    @Override
	    public LongWritable getCurrentKey() throws IOException,
	        InterruptedException {
	      newKey.set(1);
	      return newKey;
	    }

	    Text newValue = new Text();
	    
	    @Override
	    public Text getCurrentValue() throws IOException, InterruptedException {
	      newValue.set("");
	      return newValue;
	    }

	    @Override
	    public float getProgress() throws IOException, InterruptedException {

	      
	      return 0.5f;
	    }

	    @Override
	    public void close() throws IOException {
	      // TODO Auto-generated method stub

	    }
	  }

	  @Override
	  public List<InputSplit> getSplits(JobContext context) throws IOException,
	      InterruptedException {

	    int splits = context.getConfiguration().getInt(MAPRED_MAP_TASKS, 2);

	    List<InputSplit> splitList = new ArrayList<InputSplit>();

	    for (int i = 0; i < splits; i++) {
	      splitList.add(new ConfigurableInputSplit());
	    }

	    return splitList;
	  }

	  @Override
	  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	      TaskAttemptContext context) throws IOException, InterruptedException {
	    ConfigurableRecordReader reader = new ConfigurableRecordReader();
	    reader.initialize((ConfigurableInputSplit) (split), context);

	    return reader;
	  }

	  
	  
	  public static void setMapperNumber(Job job, int numberOfMappers) {

	    job.getConfiguration().set(MAPRED_MAP_TASKS, Integer.toString(numberOfMappers));
	  }

	}