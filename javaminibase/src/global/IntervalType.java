package global;

public class IntervalType {

	public static final int MINIMUM = -100000;
	public static final int MAXIMUM = 100000;
	
	int start;
	int end;
	int level; // -1 is an invalid value, levels start from 0.

	//	boolean isValid;
	
	//Default Contructor
	public IntervalType() {
		this.start = -1;
		this.end = -1;
		this.level = 0;
	}
  
	public void assign(int start, int end, int level) {
		try {
			if (start < MINIMUM || end > MAXIMUM) {
				throw new Exception("limit exceeded");
			}
			this.start = start;
			this.end = end;
			this.level = level;
//			this.isValid = true;
		} catch (Exception e) {
			this.start = 0;
			this.end = 0;
			this.level = -1;
//			this.isValid = false;
		}
	}
  
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}	
}
