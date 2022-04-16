package map_reduce_sys.ressource;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.componant.ComponentCalcul;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.map.ComponentMap;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public class GestionResourceInboundPort extends AbstractInboundPort implements ManagementI {

	
	private static final long serialVersionUID = 1L;

	public GestionResourceInboundPort(ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner);
	
	}
	public GestionResourceInboundPort(String uri,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner);
	
	}


	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
			return  this.getOwner().handleRequest(r->((ManagementI)r).runTaskResource(function,t));	
	}
	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

}
