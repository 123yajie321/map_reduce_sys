package map_reduce_sys;

import java.util.ArrayList;



public class Source implements Resource {
	
	private int dimention;
	
	private ArrayList<?> data;
	
//	public Source(int dimention) {
//		this.dimention=dimention;
//		data=new ArrayList<>();
//	}

	@Override
	public int getDimention() {
		
		return dimention;
	}

	@Override
	public ArrayList<?> getData() {
	
		return data;
	}

}
