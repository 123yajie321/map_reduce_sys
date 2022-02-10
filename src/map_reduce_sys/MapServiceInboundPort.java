package map_reduce_sys;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;


public class MapServiceInboundPort extends AbstractInboundPort implements SendTupleServiceI {
	
	private static final long serialVersionUID=1L;


	public MapServiceInboundPort(ComponentI owner) throws Exception {
		super(SendTupleServiceI.class, owner);
		// TODO Auto-generated constructor stub
	}


	@Override
	public boolean tupleSender(Tuple t, String uri) {
		
		
		return ;
	}
	
	
	

	
	public MapServiceInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri, CalculServiceI.class,owner);
	}
	
	@Override
	public Tuple map(Callable<?> f, Tuple tuple) throws Exception {
		
		return this.getOwner().handleRequest(cg->((ComponentGestion)cg).map(f, tuple));
	}

	@Override
	public Tuple reduce(Callable<?> g, Object acc, Tuple tuple)throws Exception {
		return this.getOwner().handleRequest(cg->((ComponentGestion)cg).reduce(g, acc, tuple));
	}

	@Override
	public ArrayList<Tuple> generate_data(Callable<?> g)throws Exception {
		
		return this.getOwner().handleRequest(cg->((ComponentGestion)cg).generate_data(g));
	}


	@Override
	public Tuple tupleSender() {
		// TODO Auto-generated method stub
		return null;
	}

}
