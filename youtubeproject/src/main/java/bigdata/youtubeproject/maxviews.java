package bigdata.youtubeproject;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


	public class maxviews {
		
		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "videos by viewers");
			job.setJarByClass(maxviews.class);
			job.setMapperClass(MyMap.class);
			job.setReducerClass(MyReduce.class);
		   	job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			}

		public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
			
			
			private Text category = new Text();
			private  IntWritable views = new IntWritable();

			public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
 
				String record = value.toString();
				String[] fields = record.split("\t");
				if(fields.length > 1){
					category.set(fields[0]);
   
					int f = Integer.parseInt(fields[5]);
					
					views.set(f);
					
				}

				context.write(category, views);
			}

		}
		public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

			public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

			 int maxValue = Integer.MIN_VALUE;
			 
			 for (IntWritable value : values) {
				 
				 maxValue = Math.max(maxValue, value.get());
	 			}
			 
			 context.write(key, new IntWritable(maxValue));
		}
	}


}
