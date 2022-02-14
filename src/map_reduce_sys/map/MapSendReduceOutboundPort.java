package map_reduce_sys.map;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class MapSendReduceOutboundPort extends AbstractOutboundPort implements SendTupleServiceI {

	private static final long serialVersionUID=1L;
	
	public MapSendReduceOutboundPort( ComponentI owner)
			throws Exception {
		super(SendTupleServiceI.class, owner);
		
	}

	public  MapSendReduceOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceI.class, owner);
	}

	
	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return ((SendTupleServiceI)this.getConnector()).tupleSender(t);
	}
	

}
