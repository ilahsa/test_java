package demo9;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.WritableComparable;

public class UserBean implements WritableComparable<UserBean>{
	private String userid;
	private String stationid;
	private String startTime;
	private int staytime;
	private String flag="false";
	
	public UserBean() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public void set(String userid, String stationid, String startTime,
			int staytime) {
		this.userid = userid;
		this.stationid = stationid;
		this.startTime = startTime;
		this.staytime = staytime;
	}
	
	
	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getStationid() {
		return stationid;
	}

	public void setStationid(String stationid) {
		this.stationid = stationid;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public int getStaytime() {
		return staytime;
	}

	public void setStaytime(int staytime) {
		this.staytime = staytime;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.userid=in.readUTF();
		this.stationid=in.readUTF();
		this.startTime=in.readUTF();
		this.staytime=in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userid);
		out.writeUTF(stationid);
		out.writeUTF(startTime);
		out.writeInt(staytime);
	}
	
	@Override
	public int compareTo(UserBean o) {
		int temp=this.userid.compareTo(o.getUserid()) ;
		if(temp==0){
			temp=this.stationid.compareTo(o.getStationid());
		}
		return temp;
	}

	@Override
	public String toString() {
		return userid+","+stationid+","+startTime+","+staytime;
	}
	public static void main(String[] args) throws ParseException {
		String s="2016-06-11 09:58:20";
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:hh:ss");
		sdf.parse(s);
	}
}
