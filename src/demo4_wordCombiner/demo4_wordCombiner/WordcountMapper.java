package demo4_wordCombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 求wordcount
 * KEYIN: 默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
 * 但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
 * 
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 * 
 * KEYOUT：是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
 * VALUEOUT：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
 * 
 * @author
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	IntWritable c=new IntWritable(1);
	Text t=new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] fields=line.split(" ");
		for(int i=0;i<fields.length;i++){
			t.set(fields[i]);
			context.write(t, c);
		}
		
	}
	
}
