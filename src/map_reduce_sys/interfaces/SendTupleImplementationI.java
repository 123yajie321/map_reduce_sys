package map_reduce_sys.interfaces;

import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.structure.Tuple;

public interface SendTupleImplementationI {
	
	public void send_Tuple(SendTupleOutboundPort port, Tuple result) throws Exception;
	

}
