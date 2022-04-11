package map_reduce_sys.job;

import java.util.concurrent.Callable;



public interface Job  {

	public Object f(Object ...args) ;
	  
	public Object g(Object ...args);
	

}
