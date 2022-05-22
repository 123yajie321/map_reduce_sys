package map_reduce_sys.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.componant.ComponentGestion;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>ReceiveTupleInboundPort</code> implements an inbound port for the
 * <code>SendTupleServiceCI</code> component interface 
 * which allows getsion component  to receive tuple from reduce component
 * @author	Yajie LIU, Zimeng ZHANG
 */

public class ReceiveTupleInboundPort extends AbstractInboundPort implements SendTupleServiceCI {


	private static final long serialVersionUID = 1L;

	public ReceiveTupleInboundPort(String uri,ComponentI owner)
			throws Exception {
		super(uri,SendTupleServiceCI.class, owner);
	}
	
	public ReceiveTupleInboundPort(ComponentI owner)
			throws Exception {
		super(SendTupleServiceCI.class, owner);
	}

	@Override
	public void tupleSender(Tuple t) throws Exception {
		 this.getOwner().handleRequest(m->((ComponentGestion)m).recieve_Tuple(t));
	}

}
