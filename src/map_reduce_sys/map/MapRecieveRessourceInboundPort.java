package map_reduce_sys.map;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.RecieveTupleServiceI;

public class MapRecieveRessourceInboundPort extends AbstractInboundPort implements RecieveTupleServiceI {

	private static final long serialVersionUID=1L;

	public MapRecieveRessourceInboundPort(ComponentI owner) throws Exception {
		super(RecieveTupleServiceI.class, owner);
		assert owner instanceof ComponentMap;

	}

	
	
	public MapRecieveRessourceInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri, RecieveTupleServiceI.class,owner);
		assert owner instanceof ComponentMap;
	}
	

	@Override
	public boolean tupleReciever(Tuple t) throws Exception {
		//return this.getOwner().handleRequest(m->((ComponentMap)m).recieve_Tuple(t));	
		
		return((ComponentMap)this.getOwner()).recieve_Tuple(t);
	}

}
