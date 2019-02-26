package global;

public class intervaltype {
	
//	public static final int MINIMUM = -100000;
//	public static final int MAXIMUM =  100000;
	
	public int start, end;
	
	public intervaltype(int s, int e) {
		start = s;
		end = e;
	}
	
	public void assign(int start, int end) {
		this.start = start;
		this.end = end;
	}
	

}
