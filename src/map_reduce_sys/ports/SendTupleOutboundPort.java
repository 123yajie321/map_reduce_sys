package map_reduce_sys.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Tuple;

public class SendTupleOutboundPort extends AbstractOutboundPort implements SendTupleServiceI {

	
	private static final long serialVersionUID = 1L;

	public SendTupleOutboundPort( ComponentI owner)
			throws Exception {
		super(SendTupleServiceI.class, owner);
		// TODO Auto-generated constructor stub
	}

	public SendTupleOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceI.class, owner);
	}
	

	

	@Override
	public void  tupleSender(Tuple t) throws Exception {
		 ((SendTupleServiceI)this.getConnector()).tupleSender(t);
	}

	
	
}