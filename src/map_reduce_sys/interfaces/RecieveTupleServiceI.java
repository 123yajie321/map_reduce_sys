package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import map_reduce_sys.Tuple;

public interface RecieveTupleServiceI extends OfferedCI {
	
	public boolean tupleReciever(Tuple t)throws Exception;

}
