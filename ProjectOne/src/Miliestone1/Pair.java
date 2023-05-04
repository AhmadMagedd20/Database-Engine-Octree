package Miliestone1;

import java.io.Serializable;

public class Pair implements Serializable {
	private Object min;
	private Object max;
	
	public Pair(Object min , Object max) {
		this.min = min;
		this.max = max;
		
	}

	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}
	
	public String toString() {
		return "Min: " + min  + " Max: " + max;
	}
	
	
	
	
	
	

}
