package map_reduce_sys;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;

public class MapServiceOutboundPort extends AbstractOutboundPort implements SendTupleServiceI {
	
	private static final long serialVersionUID=1L;


	public MapServiceOutboundPort(ComponentI owner) throws Exception {
		super(SendTupleServiceI.class, owner);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Tuple tupleSender() {
		// TODO Auto-generated method stub
		return null;
	}

}
