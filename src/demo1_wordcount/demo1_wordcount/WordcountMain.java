package demo1_wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class WordcountMain {
	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
    //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
	/*conf.set("mapreduce.framework.name", "local");*/
	
	//本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
	//到底在哪里，就看以下两行配置你用哪行，默认就是file:///
	/*conf.set("fs.defaultFS", "hdfs://mini1:9000/");*/
	/*conf.set("fs.defaultFS", "file:///");*/
	
	
	
	//运行集群模式，就是把程序提交到yarn中去运行
	//要想运行为集群模式，以下3个参数要指定为集群上的值
/*	conf.set("mapreduce.framework.name", "yarn");
	conf.set("yarn.resourcemanager.hostname", "mini1");
	conf.set("fs.defaultFS", "file:///");*/
		Job j=Job.getInstance(conf);
	/*	j.setJar("D:/hadoop.jar");*/
		j.setJarByClass(WordcountMain.class);
		j.setMapperClass(WordcountMapper.class);
		j.setReducerClass(WordcountReducer.class);
		
		//map端的输出的类型
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(IntWritable.class);
		//最终的输出的类型
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		boolean b = j.waitForCompletion(true);
		System.exit(b?0:1);
		
	}
}
