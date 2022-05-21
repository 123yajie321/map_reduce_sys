package map_reduce_sys.ports;



import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public class GestionOutboundPort extends AbstractOutboundPort implements ManagementI {

	private static final long serialVersionUID=1L;


	public GestionOutboundPort(ComponentI owner) throws Exception {
		super(ManagementI.class, owner);
	
	}
	
	public GestionOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, ManagementI.class, owner);
	}



	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		return ((ManagementI)this.getConnector()).runTaskResource(function, t);
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
	
			return ((ManagementI)this.getConnector()).runTaskMap(function,t);
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		 return ((ManagementI)this.getConnector()).runTaskReduce(function, t,nature);
	}

	@Override
	public void DoPluginPortConnection() throws Exception {
		  ((ManagementI)this.getConnector()).DoPluginPortConnection();
		
	}

	/*@Override
	public void createPluginResource(String managementResourceInboundPort, int nb,
			Function<Integer, Tuple> data_generator, String mapReceiveTupleinboundPorturi, int pluginId)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createPluginMap(String managementMapInboundPort, int nb, Function<Tuple, Tuple> fonction_map,
			String mapReceiveTupleinboundPorturi, String ReduceReceiveTupleinboundPortUri, int pluginId)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createPluginReduce(String managementReduceInboundPort, int nb,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, String ReduceReceiveTupleinboundPorturi,
			String sendResultinboundPortUri, int pluginId) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	*/
	

	

}
