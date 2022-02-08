package map_reduce_sys;

import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;

public  class SimpleJob  {
	
	protected ArrayList<Tuple> data;
	protected Nature nature;
	protected Function <Tuple,Tuple> f;
	protected BiFunction < Tuple,Tuple,Tuple>g;
	
	
	protected SimpleJob(ArrayList<Tuple>data,Function <Tuple,Tuple> f,BiFunction < Tuple,Tuple,Tuple> g,Nature nature) {
		this.data=data;
		this.f=f;
		this.g=g;
		this.nature=nature;
		
	}
	
	public ArrayList<Tuple> getData() {
		return data;
	}
	
     
	
	
	public Function <Tuple,Tuple> getF() {
		return f;
	}
	
	public BiFunction < Tuple,Tuple,Tuple> getG() {
		return g;
	}
	
	
	public Nature getNature() {
			
			return nature;
		}

	
	
	

}
