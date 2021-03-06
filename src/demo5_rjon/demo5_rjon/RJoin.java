package demo5_rjon;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class RJoin {
	static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
		InfoBean bean=new InfoBean();
		Text k=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			System.out.println(line);
			FileSplit fsplit = (FileSplit) context.getInputSplit();
			String name = fsplit.getPath().getName();
			String pid;
			if(name.startsWith("order")){
				String[] split = line.split(",");
				pid=split[2];
				bean.set(Integer.parseInt(split[0]), split[1], pid, Integer.parseInt(split[3]), "", 0, 0, "0");
			}else{
				String[] split=line.split(",");
				pid=split[0];
				bean.set(0, "", pid,0, split[1], 0, 0, "1");
			}
			k.set(pid);
			context.write(k, bean);
		}
	}
	static class RJoinReducer extends Reducer<Text, InfoBean,InfoBean, NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,
				Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString());
			InfoBean pdBean = new InfoBean();
			ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
			for(InfoBean b:values){
				if(b.getFlag().equals("1")){
					try {
						BeanUtils.copyProperties(pdBean, b);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else{
					InfoBean odBean = new InfoBean();
					try {
						BeanUtils.copyProperties(odBean, b);
					} catch (IllegalAccessException | InvocationTargetException e) {
						e.printStackTrace();
					}
					orderBeans.add(odBean);
				}
			}

			// 拼接两类数据形成最终结果
			for (InfoBean bean : orderBeans) {

				bean.setPname(pdBean.getPname());
				bean.setCategory_id(pdBean.getCategory_id());
				bean.setPrice(pdBean.getPrice());

				context.write(bean, NullWritable.get());
			}

		}
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		 job.setJarByClass(RJoin.class);
   //		job.setJar("c:/join.jar");

		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(RJoinMapper.class);
		job.setReducerClass(RJoinReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path("C:/work/java/first/src/demo5_rjon/input"));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path("C:/work/java/first/src/demo5_rjon/output"));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.out.println("result "+res);
		System.exit(res ? 0 : 1);

	}
}
