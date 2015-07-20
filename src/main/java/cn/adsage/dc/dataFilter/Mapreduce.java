package cn.adsage.dc.dataFilter;

import java.io.IOException;
import java.util.ArrayList;

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

public class Mapreduce extends Configured implements Tool{
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/filterData";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result";
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hadoop jar xxx.jar <index of the count field> <number to display>");  
		    System.exit(-1);  
		}
		
		//接收参数，记录需要统计字段的下标
		String field = args[0];
		//接收参数，记录临界值的大小
		String num = args[1];
		Configuration conf = new Configuration();
		//利用configuration来给map和reduce函数进行传参
		conf.set("num", num);
		conf.set("field", field);
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, Mapreduce.class.getSimpleName());
			job.setJarByClass(Mapreduce.class);
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
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
			ToolRunner.run(new Mapreduce(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text text = new Text();
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//利用context来获取configuration对象
			Configuration conf = context.getConfiguration();
			//利用configuration对象来获取参数
			String field = conf.get("field");
			//把获取到的字符串参数转化为整型
			int fieldIndex = Integer.parseInt(field);
			//把每一条记录进行格式化，再进行切分
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			//获取想要统计的字段
			String k2 = split[fieldIndex];
			text.set(k2);
			//(需要统计的字段,整条记录)的形式写出
			context.write(text, v1);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, LongWritable>{
		LongWritable value = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//利用context来获取configuration对象
			Configuration conf = context.getConfiguration();
			//利用configuration对象来获取参数，并转化为long类型
			long num = Long.parseLong(conf.get("num"));
			
			long sum = 0;
			//构造一个ArrayList
			ArrayList<String> arrayList = new ArrayList<String>();
			//获取同一个key的记录数
			for (Text v2 : v2s) {
				//因为需要第二次迭代，所以要把迭代器的元素重新放入一个集合中，否则第二次迭代为空
				arrayList.add(v2.toString());
				sum += 1;
			}
			//如果统计的次数大于给定的临界值，则把记录输出
			if (sum >= num) {
				for (String v2 : arrayList) {
					value.set(sum);
					context.write(new Text(v2), value);
				}
			}
		}
	}
}