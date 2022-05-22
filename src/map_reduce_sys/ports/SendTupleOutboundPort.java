package map_reduce_sys.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>SendTupleOutboundPort</code> implements an outbound port for the
 * <code>SendTupleServiceCI</code> component interface.
 * @author	Yajie LIU, Zimeng ZHANG
 */


public class SendTupleOutboundPort extends AbstractOutboundPort implements SendTupleServiceCI {

	
	private static final long serialVersionUID = 1L;

	public SendTupleOutboundPort( ComponentI owner)
			throws Exception {
		super(SendTupleServiceCI.class, owner);
		
	}

	public SendTupleOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceCI.class, owner);
	}
	

	

	@Override
	public void  tupleSender(Tuple t) throws Exception {
		 ((SendTupleServiceCI)this.getConnector()).tupleSender(t);
	}

	
	
}
