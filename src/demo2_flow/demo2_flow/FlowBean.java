package demo2_flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private long upFlow;// 上行流量
	private long dFlow;// 下行流量
	private long sumFlow;
	
	//反序列化时，需要反射会调用一个空参构造
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	public FlowBean(long upFlow, long dFlow) {
		super();
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow=upFlow+dFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}
	
	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		
	}
	/**
	 * 反序列化方法
	 * 注意：反序列化的顺序跟序列化的顺序是一样
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readLong();
		dFlow=in.readLong();
		sumFlow=in.readLong();
		
	}

	@Override
	public String toString() {
		return upFlow+"\t"+dFlow+"\t"+sumFlow;
	}
	
	
}
