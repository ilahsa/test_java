package first;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by user on 2017/2/8.
 */
public class InputOutputSame {
	/**
	 * Created by user on 2017/2/8.
	 */
	static class MapperInputOutputSame extends Mapper<Object, Text, NullWritable, Text>  {
	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        System.out.println(value.toString());
	        System.out.println("-----------------------------");
	        context.write(NullWritable.get(),value);
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
            conf.setStrings("fs.local.block.size", "1073741824");
            Job job = Job.getInstance(conf);
            job.setJobName("Statistics Data");
            job.setJarByClass(MainTest02.class);
            job.setMapperClass(MapperInputOutputSame.class);
            job.setOutputKeyClass(NullWritable.class);
            //job.setOutputValueClass(Text.class);
            //job.setInputFormatClass(TextInputFormat.class);
            // 判断output文件夹是否存在，如果存在则删除
            Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
            FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
            }

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            System.out.println("job finished");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}


