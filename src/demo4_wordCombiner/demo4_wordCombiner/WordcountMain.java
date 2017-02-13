package demo4_wordCombiner;


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
		//如果跑本地
		conf.set("mapreduce.framework.name", "local");
		//本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
		conf.set("fs.defaultFS","file:///");
		
		
		//如果要讲代码提交的集群上 需要设置一下两个参数 1 指定yarn 
		//2 指定resoucemanager在那个机器上
		//3指文件的来源
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "shizhan");
		conf.set("fs.defaultFS","hdfs://shizhan:9000")
		*/
		Job j=Job.getInstance(conf);
		
		j.setJarByClass(WordcountMain.class);
		
		j.setMapperClass(WordcountMapper.class);
		j.setReducerClass(WordcountReducer.class);
		
		j.setCombinerClass(WordcountCombiner.class);
		
		//map端的输出的类型
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(IntWritable.class);
		//最终的输出的类型
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(j, new Path("C:/work/java/first/src/demo4_wordCombiner/words.txt"));
		FileOutputFormat.setOutputPath(j, new Path("C:/work/java/first/src/demo4_wordCombiner/output"));
		
		boolean b = j.waitForCompletion(true);
		System.exit(b?0:1);
		
	}
}
