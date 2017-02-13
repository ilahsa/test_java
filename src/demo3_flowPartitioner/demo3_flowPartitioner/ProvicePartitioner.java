package demo3_flowPartitioner;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定分区，k v对应的是map端输出的k v
 * @author lp
 *
 */
public class ProvicePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String,Integer> dict=new HashMap<>();
	static{
		dict.put("136", 0);
		dict.put("137", 1);
		dict.put("138", 2);
		dict.put("139", 3);
	}
	@Override
	public int getPartition(Text key, FlowBean bean, int numpartition) {
		String phone = key.toString();
		String sb = phone.substring(0, 3);
		 Integer id=dict.get(sb);
		return id==null?4:id;
	}

}
