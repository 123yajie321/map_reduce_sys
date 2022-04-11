package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.structure.Tuple;

public interface SendTupleImplementationI {
	
	public void send_Tuple(SendTupleOutboundPort port, Tuple result) throws Exception;
	

}
