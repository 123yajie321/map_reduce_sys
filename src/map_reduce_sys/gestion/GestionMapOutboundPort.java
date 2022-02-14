package map_reduce_sys.gestion;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class GestionMapOutboundPort extends AbstractOutboundPort implements SendTupleServiceI {

	private static final long serialVersionUID=1L;


	public GestionMapOutboundPort(ComponentI owner) throws Exception {
		super(SendTupleServiceI.class, owner);
		// TODO Auto-generated constructor stub
	}
	
	public GestionMapOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceI.class, owner);
	}

	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return ((SendTupleServiceI)this.getConnector()).tupleSender(t);
	}

}
