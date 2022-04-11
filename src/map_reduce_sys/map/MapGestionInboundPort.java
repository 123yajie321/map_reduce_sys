package map_reduce_sys.map;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.reduce.ComponentReduce;
import map_reduce_sys.structure.Tuple;

public class MapGestionInboundPort extends AbstractInboundPort implements ManagementI {

	private static final long serialVersionUID=1L;

	public MapGestionInboundPort(ComponentI owner) throws Exception {
		super(RecieveTupleServiceI.class, owner);
		assert owner instanceof ComponentGestion;

	}

	
	
	public MapGestionInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri, RecieveTupleServiceI.class,owner);
	}






	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return this.getOwner().handleRequest(m->((ComponentMap)m).runTaskMap(function,(int) t.getIndiceData(0))
				
				);
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	

	
}
