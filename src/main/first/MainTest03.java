package first;


import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapred.DeprecatedLzoTextInputFormat;
import com.hadoop.mapreduce.LzoTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by user on 2017/2/8.
 */
public class MainTest03 {

    public static void main(String[] args) {
        try {
           // args = new String[]{"C:\\Users\\user\\Desktop\\data_l\\aa", "D:\\mr\\t_new26"};
            if (args.length != 2) {
                System.err.println("Usage: Statistics <in> <out>");
                System.exit(2);
            }
            Configuration conf = new Configuration();
            conf.setStrings("fs.local.block.size", "1073741824");
            Job job = Job.getInstance(conf);
            job.setJobName("Statistics Data");
            job.setJarByClass(MainTest02.class);
            job.setMapperClass(MapperTest02.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            job.setInputFormatClass(SequenceFileInputFormat.class);
            //job.setInputFormatClass(LzoTextInputFormat.class);
            
            //job.setInputFormatClass(LzoSequenceInputFormat.class);

            //job.setInputFormatClass(SequenceFileAsTextInputFormat.class);

            //job.setOutputFormatClass(TextOutputFormat.class);
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);

             //FileOutputFormat.setCompressOutput(job, true);
            // FileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);

            // 判断output文件夹是否存在，如果存在则删除
            Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
            FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
            }

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
