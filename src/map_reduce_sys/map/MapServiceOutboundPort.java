package map_reduce_sys.map;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Tuple;

public class MapServiceOutboundPort extends AbstractOutboundPort implements SendTupleServiceI {
	
	private static final long serialVersionUID=1L;


	public MapServiceOutboundPort(ComponentI owner) throws Exception {
		super(SendTupleServiceI.class, owner);
		// TODO Auto-generated constructor stub
	}
	
	public MapServiceOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceI.class, owner);
	}

	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return ((SendTupleServiceI)this.getConnector()).tupleSender(t);
	}
	

}
