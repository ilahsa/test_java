package demo9;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import demo5_rjon.RJoin;

public class Jizhan {
	static class JizhanMapper extends Mapper<LongWritable, Text, UserBean, UserBean>{
		UserBean b=new UserBean();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			b.set(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]));
			context.write(b, b);
		}
	}
	static class JizhanReducer extends Reducer<UserBean, UserBean, UserBean, NullWritable> {

		@Override
		protected void reduce(UserBean key, Iterable<UserBean> values,
				Context context)
				throws IOException, InterruptedException {
			//将bean按时间先后顺序排序
				List<UserBean> a=new ArrayList<UserBean>();
				for(UserBean v:values){
					UserBean u=new UserBean();
					try {
						BeanUtils.copyProperties(u, v);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					a.add(u);
				}
				//将bean按时间先后顺序排序
				Collections.sort(a, new Comparator<UserBean>() {
					@Override
					public int compare(UserBean o1, UserBean o2) {
						try {
							Date d1 = toDate(o1.getStartTime());
							Date d2 = toDate(o2.getStartTime());
							return d1.compareTo(d2);
						} catch (ParseException e) {
							e.printStackTrace();
							return 0;
						}
					}
				});
				for(int i=0;i<a.size();i++){
					UserBean u=a.get(i);
					//如果只要一条数据，那么直接输出
					if(a.size()==1){
						context.write(u, NullWritable.get());
						break;
					}
					//如果是第一条 不做处理
					if(i==0){
						continue;
					}
					String nowSid=u.getStationid();
					//上一次的bean
					UserBean lastbean = a.get(i-1);
					String lostSid=lastbean.getStationid();
					//判断当前的bean和上一次的bean的机架是否是一个
					if(nowSid.equals(lostSid)){
						lastbean.setStaytime(u.getStaytime()+lastbean.getStaytime());
						//这个字段用来做标示，防止下次如果的机架和 这个机架不一样时。会走入else，但是已经用过的 不做输出
						lastbean.setFlag("true");
						context.write(lastbean, NullWritable.get());
						a.remove(i);
						i--;
						continue;
					}else{
						//判断上一次的是否已经用过了
						if(!lastbean.getFlag().equals("true")){
							context.write(lastbean, NullWritable.get());
						}
					}
					//如果是最后一行 直接输出
					if(i==a.size()-1){
						context.write(u, NullWritable.get());
					}
					
				}
		}
		private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}
		
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		 job.setJarByClass(RJoin.class);
   //		job.setJar("c:/join.jar");

		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(JizhanMapper.class);
		job.setReducerClass(JizhanReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(UserBean.class);
		job.setMapOutputValueClass(UserBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(UserBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(UserPartition.class);
		job.setGroupingComparatorClass(UserGroupingComparator.class);
		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path("C:/work/java/first/src/demo9/input"));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path("C:/work/java/first/src/demo9/output"));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
