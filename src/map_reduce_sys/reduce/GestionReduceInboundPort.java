package map_reduce_sys.reduce;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.map.ComponentMap;

public class GestionReduceInboundPort extends AbstractInboundPort implements ManagementI {

	public GestionReduceInboundPort(ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner);
	
	}
	public GestionReduceInboundPort(String uri,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner);
	
	}

	
	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
	return this.getOwner().handleRequest(r->((ComponentReduce)r).runTaskReduce(function,(int)t.getIndiceData(0)));	
	}

}
