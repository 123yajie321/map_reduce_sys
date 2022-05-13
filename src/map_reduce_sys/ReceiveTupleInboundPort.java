package map_reduce_sys;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Tuple;

public class ReceiveTupleInboundPort extends AbstractInboundPort implements SendTupleServiceI {


	private static final long serialVersionUID = 1L;

	public ReceiveTupleInboundPort(String uri,ComponentI owner)
			throws Exception {
		super(uri,SendTupleServiceI.class, owner);
	}
	
	public ReceiveTupleInboundPort(ComponentI owner)
			throws Exception {
		super(SendTupleServiceI.class, owner);
	}

	@Override
	public void tupleSender(Tuple t) throws Exception {
		 this.getOwner().handleRequest(m->((ComponentGestion)m).recieve_Tuple(t));
	}

}
