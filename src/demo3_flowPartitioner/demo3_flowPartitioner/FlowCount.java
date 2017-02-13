package demo3_flowPartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 计算流量
 * @author lp
 *
 */
class FlowCount {
	
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		Text t=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
				String line=value.toString();
				String[] fields=line.split("\t");
				String phone=fields[1];
				Long upFlow=Long.parseLong(fields[fields.length-3]);
				Long dFlow=Long.parseLong(fields[fields.length-2]);
				t.set(phone);
				context.write(t, new FlowBean(upFlow, dFlow));
			
		}
	}
	static class FlowContReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {
			long sum_upFlow=0;
			long sum_dFlow=0;
			for(FlowBean b:values){
				sum_upFlow+=b.getUpFlow();
				sum_dFlow=b.getdFlow();
			}
			FlowBean resultBean=new FlowBean(sum_upFlow,sum_dFlow);
			//一个对象写入到文本文件会调用对象的toString 的方法
			context.write(key, resultBean);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowContReducer.class);
		
		job.setPartitionerClass(ProvicePartitioner.class);
		job.setNumReduceTasks(5);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("C:/work/java/first/src/demo3_flowPartitioner/flow.dat"));
		FileOutputFormat.setOutputPath(job, new Path("C:/work/java/first/src/demo3_flowPartitioner/output"));
		
		boolean b = job.waitForCompletion(true);
		System.out.println("result "+b);
		System.exit(b?0:1);
		
	}
}
