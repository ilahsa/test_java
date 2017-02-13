package demo9;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**\
 * 利用reduce端的GroupingComparator来实现将一组bean看成相同的key
 * @author lp
 *
 */
public class UserGroupingComparator extends WritableComparator{

	public UserGroupingComparator() {
		super(UserBean.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserBean abean=(UserBean) a;
		UserBean bbean=(UserBean) b;
		return abean.getUserid().compareTo(bbean.getUserid());
	}

	
	
}
