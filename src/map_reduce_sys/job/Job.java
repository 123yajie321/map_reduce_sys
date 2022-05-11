package map_reduce_sys.job;

import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;

import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public  class Job  {
	
	protected Nature nature;
	protected Function <Tuple,Tuple> function_map;
	protected BiFunction <Tuple,Tuple,Tuple>function_reduce;
	protected Function<Integer, Tuple> data_generator;
	protected Tuple data_size  ;
	
	
	public Job(Function<Integer, Tuple> s,Function <Tuple,Tuple> f,BiFunction < Tuple,Tuple,Tuple> g,Nature nature,Tuple size) {
	
		this.function_map=f;
		this.function_reduce=g;
		this.data_generator=s;
		this.nature=nature;
		this.data_size=size;
		
	}
		
	
	public Function <Tuple,Tuple> getFunctionMap() {
		return function_map;
	}
	
	public BiFunction < Tuple,Tuple,Tuple> getFunctionReduce() {
		return function_reduce;
	}
	
	public Function<Integer, Tuple> getDataGenerator(){
		return data_generator;
		
	}
	
	public Nature getNature() {
			
			return nature;
	}
	public Tuple getDataSize() {
		
		return data_size;
	}

	
	
	

}
