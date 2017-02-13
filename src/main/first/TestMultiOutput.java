package first;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TestMultiOutput {
	static class MapperTestMultiOutput extends Mapper<Object, Text, Text, Text>  {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println(value.toString());
			String[] strs = value.toString().split(";");
			context.write(new Text(strs[0]),new Text(strs[1]));
		}
	}

	static class ReduceTestMultiOutput extends Reducer<Text,Text,Text,Text>{
		/** 
		 * 设置多个文件输出 
		 * */ 
		private MultipleOutputs<Text, Text> mos; 

		@Override 
		protected void setup(Context context) 
				throws IOException, InterruptedException { 
			mos=new MultipleOutputs<Text, Text>(context);//初始化mos 
			super.setup(context);
		} 
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			System.out.println("kkkk "+key.toString());

			for(Text t:values){ 
				System.out.println("ggg "+t.toString());
				if(key.toString().equals("china")){ 
					System.out.println("ttt");
					mos.write("china", key,t); 

				} else if(key.toString().equals("us")){ 
					mos.write("us",key,t);    
				} 
			} 
		}
		@Override 
		protected void cleanup( 
				Context context) 
						throws IOException, InterruptedException { 
			mos.close();//释放资源 
			super.cleanup(context);
		} 
	}
	// 输入和输出完全一致
	public static void main(String[] args) {
		try {
			if (args.length != 2) {
				System.err.println("Usage: Statistics <in> <out>");
				System.exit(2);
			}
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			job.setJobName("Statistics Data");
			job.setJarByClass(MainTest02.class);
			job.setMapperClass(MapperTestMultiOutput.class);
			job.setReducerClass(ReduceTestMultiOutput.class);

			//map端的输出的类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//最终的输出的类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//job.setInputFormatClass(TextInputFormat.class);
			// 判断output文件夹是否存在，如果存在则删除
			Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
			FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
			}
			
			MultipleOutputs.addNamedOutput(job, "china",
					TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "us",
					TextOutputFormat.class, Text.class, Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			Boolean ret = job.waitForCompletion(true);
			System.out.println("job finished " + ret);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


