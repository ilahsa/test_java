package demo1_wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * KEYIN, VALUEIN 对应  mapper输出的KEYOUT,VALUEOUT类型对应
 * 
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 * KEYOUT是单词
 * VLAUEOUT是总次数 
 * @author
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			  int  sum=0;
			  for(IntWritable value:values){
				  sum+=value.get();
			  }
			  context.write(key, new IntWritable(sum));
		}
}
