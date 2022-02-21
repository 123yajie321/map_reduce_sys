package map_reduce_sys.reduce;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.map.ComponentMap;

public class ReduceReciveMapInboundPort extends AbstractInboundPort implements RecieveTupleServiceI {

	public ReduceReciveMapInboundPort( ComponentI owner)
			throws Exception {
		super(RecieveTupleServiceI.class, owner);
		assert owner instanceof ComponentReduce;
		
	}
	public ReduceReciveMapInboundPort(String uri ,ComponentI owner)
			throws Exception {
		super(uri,RecieveTupleServiceI.class, owner);
		assert owner instanceof ComponentReduce;
		
	}

	@Override
	public boolean tupleReciever(Tuple t) throws Exception {
		return this.getOwner().handleRequest(g->((ComponentReduce)g).recieve_Tuple(t));	
		//return((ComponentReduce)this.getOwner()).recieve_Tuple(t);
	}

}
