package map_reduce_sys.job;

import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;

import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public  class SimpleJob  {
	
	protected Nature nature;
	protected Function <Tuple,Tuple> function_map;
	protected BiFunction < Tuple,Tuple,Tuple>function_reduce;
	protected Function<Void, Tuple> data_generator;
	protected int data_size  ;
	
	
	public SimpleJob(Function <Tuple,Tuple> f,BiFunction < Tuple,Tuple,Tuple> g,Function<Void, Tuple> s,Nature nature,int size) {
	
		this.function_map=f;
		this.function_reduce=g;
		this.data_generator=s;
		this.nature=nature;
		this.data_size=size;
		
	}
		
	
	public Function <Tuple,Tuple> getF() {
		return function_map;
	}
	
	public BiFunction < Tuple,Tuple,Tuple> getG() {
		return function_reduce;
	}
	
	
	public Nature getNature() {
			
			return nature;
		}

	
	
	

}
