package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;

import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;
/**
 * The interface <code>ManagementCI</code> declares the signatures of the
 * implementation methods for launching calculation tasks or connection services.
 *
 * @author Yajie LIU, Zimeng ZHANG
 */
public interface ManagementCI  extends OfferedCI,RequiredCI{
	
	public boolean runTaskResource(Function<Integer, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskMap(Function<Tuple, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskReduce(BiFunction<Tuple,Tuple, Tuple>function,Tuple t,Nature nature) throws Exception;
	public void DoPluginPortConnection()throws Exception;
	
	
	
}
