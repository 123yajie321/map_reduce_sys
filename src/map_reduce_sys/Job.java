package map_reduce_sys;

import java.util.concurrent.Callable;



public interface Job  {

	public Object f(Object ...args) ;
	  
	public Object g(Object ...args);
	

}
