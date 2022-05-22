package map_reduce_sys.ports;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementCI;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>GestionOutboundPort</code> implements an outbound port for the
 * <code>ManagementCI</code> component interface.
 * @author	Yajie LIU, Zimeng ZHANG
 */


public class GestionOutboundPort extends AbstractOutboundPort implements ManagementCI {

	private static final long serialVersionUID=1L;


	public GestionOutboundPort(ComponentI owner) throws Exception {
		super(ManagementCI.class, owner);
	
	}
	
	public GestionOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, ManagementCI.class, owner);
	}



	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		return ((ManagementCI)this.getConnector()).runTaskResource(function, t);
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
	
			return ((ManagementCI)this.getConnector()).runTaskMap(function,t);
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		 return ((ManagementCI)this.getConnector()).runTaskReduce(function, t,nature);
	}

	@Override
	public void DoPluginPortConnection() throws Exception {
		  ((ManagementCI)this.getConnector()).DoPluginPortConnection();
		
	}

}
