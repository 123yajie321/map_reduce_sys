package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.Tuple;

public interface SendTupleServiceI extends RequiredCI,OfferedCI {
	
	public boolean tupleSender(Tuple t)throws Exception;
	

}
