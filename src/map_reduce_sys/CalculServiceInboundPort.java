package map_reduce_sys;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;




public class CalculServiceInboundPort extends AbstractInboundPort implements CalculServiceI {

	private static final long serialVersionUID=1L;
	
	public CalculServiceInboundPort( ComponentI owner)throws Exception {
		super(CalculServiceI.class, owner);
		
	}

	
	public CalculServiceInboundPort(String uri,ComponentI owner) throws Exception {
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

}
