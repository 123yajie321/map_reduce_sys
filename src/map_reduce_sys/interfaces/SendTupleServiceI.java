package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.Tuple;

public interface SendTupleServiceI extends RequiredCI {
	
	public void tupleSender(Tuple t)throws Exception;
	

}
