package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.structure.Tuple;

public interface SendTupleServiceI extends RequiredCI,OfferedCI {
	
	public void tupleSender(Tuple t)throws Exception;
	

}
