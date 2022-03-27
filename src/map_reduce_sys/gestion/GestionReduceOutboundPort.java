package map_reduce_sys.gestion;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class GestionReduceOutboundPort extends AbstractOutboundPort implements ManagementI {

	private static final long serialVersionUID=1L;


	public GestionReduceOutboundPort(ComponentI owner) throws Exception {
		super(SendTupleServiceI.class, owner);
		
	}
	
	public GestionReduceOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, SendTupleServiceI.class, owner);
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
		 return ((ManagementI)this.getConnector()).runTaskReduce(function, t);
	}

	

}
