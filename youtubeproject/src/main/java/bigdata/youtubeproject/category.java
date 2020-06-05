package bigdata.youtubeproject;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


	
	public class category {

		 public static void main(String[] args) throws Exception {
	     
                 Configuration conf = new Configuration();	  
	             Job job = Job.getInstance(conf, "Videos Categories in Youtube");
	             job.setJarByClass(category.class);
		         job.setMapperClass(MyMap.class);
	             job.setReducerClass(MyReduce.class);
		         job.setOutputKeyClass(Text.class);
	             job.setOutputValueClass(IntWritable.class);
	             job.setMapOutputKeyClass(Text.class);
	             job.setMapOutputValueClass(IntWritable.class);

		         FileInputFormat.addInputPath(job, new Path(args[0]));
	             FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	             job.setInputFormatClass(TextInputFormat.class);
	             job.setOutputFormatClass(TextOutputFormat.class);
                     
	             System.exit(job.waitForCompletion(true) ? 0 : 1);
	       
	    }
        //extending the Mapper default class having the arguments keyIn as LongWritable 
	    //and ValueIn as Text and KeyOut as Text and ValueOut as IntWritable

	   	 public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {
            
	    	       private Text category = new Text();

		       private final static IntWritable one = new IntWritable(1);
    

	       public void map(LongWritable key, Text value, Context context )
	            throws IOException, InterruptedException {

                    String[] fields = value.toString().split("\t");

                    if(fields.length > 7){

		//storing the category which is in the column #4
   	               category.set(fields[3]);
   	          }
		//writing the key and value into the context which will be the output of the map method.
                   context.write(category, one);
	      }

	    }
		
		//extends the default Reducer class with arguments KeyIn as Text and ValueIn as IntWritable 
		//which are same as the outputs of the mapper class and KeyOut as Text and ValueOut as IntWritbale 
		//which will be final outputs of our MapReduce program.
	    public static class MyReduce extends Reducer<Text, IntWritable,
	            Text, IntWritable> {


	       public void reduce(Text key, Iterable<IntWritable> values,Context context)
	            throws IOException, InterruptedException {

	           int sum = 0;
	           
		//for each loop is taken which will run each time for the values inside the “Iterable values”
		// which are coming from the shuffle and sort phase after the mapper phase.
	           for (IntWritable val : values) {

		//storing and calculating the sum of the values
	               sum += val.get();
	           }
	           context.write(key, new IntWritable(sum));
	       }
	   }
}
