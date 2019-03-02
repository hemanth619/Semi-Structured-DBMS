package global;

public class IntervalType {

	public static final int MINIMUM = -100000;
	public static final int MAXIMUM = 100000;
	
	public int start;
	public int end;
	public int level; // -1 is an invalid value, levels start from 0.
//	boolean isValid;
	
	public IntervalType() { }
	
	public IntervalType(int start, int end, int level) {
		this.start = start;
		this.end = end;
		this.level = level;
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
}
