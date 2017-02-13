package first;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by user on 2017/2/8.
 */
public class MapperTest02 extends Mapper<Object, Text, NullWritable, Text>  {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        context.write(NullWritable.get(),value);
    }
}
