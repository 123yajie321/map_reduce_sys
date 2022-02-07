package map_reduce_sys;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;

public interface CalculServiceI extends OfferedCI,RequiredCI{
  
	 public Tuple map(Callable<?>f,Tuple tuple) throws Exception;
	 public Tuple reduce(Callable<?> g,Object acc, Tuple tuple)throws Exception;
	 public ArrayList<Tuple> generate_data(Callable<?> g)throws Exception;
	 
 
	
	

}
