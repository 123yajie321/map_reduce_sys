package map_reduce_sys;

import fr.sorbonne_u.components.interfaces.RequiredCI;

public interface SendTupleServiceI extends RequiredCI {
	
	public Tuple tupleSender()throws Exception;
	

}
