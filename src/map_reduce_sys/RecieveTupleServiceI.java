package map_reduce_sys;

import fr.sorbonne_u.components.interfaces.OfferedCI;

public interface RecieveTupleServiceI extends OfferedCI {
	
	public Tuple tupleReciever(Tuple t)throws Exception;

}
