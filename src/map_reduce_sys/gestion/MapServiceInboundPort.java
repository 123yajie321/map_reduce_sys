package map_reduce_sys.gestion;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;


public class MapServiceInboundPort extends AbstractInboundPort implements RecieveTupleServiceI {
	
	private static final long serialVersionUID=1L;

	public MapServiceInboundPort(ComponentI owner) throws Exception {
		super(RecieveTupleServiceI.class, owner);
		assert owner instanceof ComponentGestion;

	}

	
	
	public MapServiceInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri, RecieveTupleServiceI.class,owner);
	}
	

	@Override
	public boolean tupleReciever(Tuple t) throws Exception {
		return this.getOwner().handleRequest(g->((ComponentGestion)g).recieveTuple(t));	
	}




}
