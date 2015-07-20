package cn.adsage.dc.dataFilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TestMR extends Configured implements Tool{
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/data";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result";
	
	public int run(String[] args) throws Exception {
		String num = args[0];
		Configuration conf = new Configuration();
		conf.set("num", num);
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, TestMR.class.getSimpleName());
			job.setJarByClass(TestMR.class);
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			
//			job.setNumReduceTasks(0);
			
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, INPUT_PATH);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new TestMR(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String num = conf.get("num");
			System.out.println("mapper-->"+num);
			context.write(key, new Text(num));
		}
	}

	public static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(
				LongWritable arg0,
				Iterable<Text> arg1,
				Reducer<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String num = conf.get("num");
			System.out.println("reducer-->"+num);
			context.write(arg0, new Text(num));
		}
	}


}
