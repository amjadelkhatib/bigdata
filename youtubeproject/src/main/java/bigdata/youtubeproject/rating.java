package bigdata.youtubeproject;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class rating {

    public static void main(String[] args) throws Exception {
      
 		 Configuration conf = new Configuration();     
                 Job job = Job.getInstance(conf, "videorating");
                 job.setJarByClass(rating.class);
                 job.setMapperClass(MyMap.class);
                 job.setReducerClass(MyReduce.class);
                 job.setMapOutputKeyClass(Text.class);
                 job.setMapOutputValueClass(FloatWritable.class);
                 job.setOutputKeyClass(Text.class);
                 job.setOutputValueClass(FloatWritable.class);

                 FileInputFormat.addInputPath(job, new Path(args[0]));
                 FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

		 System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

	//extending the Mapper default class having the arguments keyIn as LongWritable and ValueIn as Text 
	//and KeyOut as Text and ValueOut as FloatWritable.
    		public static class MyMap extends Mapper<LongWritable, Text, Text,
			FloatWritable> {
       			private Text video_id = new Text();
	
      			private  FloatWritable rating = new FloatWritable();
	
       		   public void map(LongWritable key, Text value, Context context )
				throws IOException, InterruptedException {
 
               		 String[] fields = value.toString().split("\t");

              		  if(fields.length > 1){
               		  video_id.set(fields[0]);
//checking whether the data in that index is numeric data or not by using a regular expression
// which can be achieved by matches function in java,
 //if it is numeric data then it will proceed and it should be a floating value as well.
                	  if(fields[6].matches("\\d+.+"))
               		 { 

               		 float f=Float.parseFloat(fields[6]); //typecasting string to float;
// storing the rating of the video in rating variable.
               		 rating.set(f);
               		 }
              	 }

                context.write(video_id, rating);
       		   }

    		}
//extends the default Reducer class with arguments KeyIn as Text and ValueIn as IntWritable 
//which are same as the outputs of the mapper class and KeyOut as Text and ValueOut as IntWritbale
 //which will be final outputs of our MapReduce program.
    		public static class MyReduce extends Reducer<Text, FloatWritable,
    				Text, FloatWritable> {

    			public void reduce(Text key, Iterable<FloatWritable> values,
    					Context context) throws IOException, InterruptedException {
          
    				float sum = 0;
    				int l=0;
    				for (FloatWritable val : values) {
                    l+=1;  //counts number of values are there for that key
                    sum += val.get();
    				}
    				
    			sum=sum/l;   //takes the average of the sum
    			
           context.write(key, new FloatWritable(sum));
       }
    }
}