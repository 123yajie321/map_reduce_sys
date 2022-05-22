package map_reduce_sys.structure;

import java.util.ArrayList;

import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;

/**
 * The class <code>Job</code>This class defines the functions
 *  and properties to be used for a complete resource_map_reduce job 
 *    
 * @author Yajie LIU, Zimeng ZHANG
 */

public  class Job  {
	/**The nature of function reduce*/
	protected Nature nature;
	/**The function used by map component*/
	protected Function <Tuple,Tuple> function_map;
	/**The function used by reduce component*/
	protected BiFunction <Tuple,Tuple,Tuple>function_reduce;
	/**The function used by resource component*/
	protected Function<Integer, Tuple> data_generator;
	/**Total amount of data of job*/
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
