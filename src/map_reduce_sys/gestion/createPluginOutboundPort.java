package map_reduce_sys.gestion;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createPluginI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public class createPluginOutboundPort extends AbstractOutboundPort implements createPluginI {

	private static final long serialVersionUID=1L;


	public createPluginOutboundPort(ComponentI owner) throws Exception {
		super(createPluginI.class, owner);
	
	}
	
	public createPluginOutboundPort(String uri, ComponentI owner)
			throws Exception {
		super(uri, createPluginI.class, owner);
	}



	
	@Override
	public void createPluginResource(String managementResourceInboundPort, int nb,
			Function<Integer, Tuple> data_generator, String mapReceiveTupleinboundPorturi, int pluginId)
			throws Exception {
		((createPluginI)this.getConnector()).createPluginResource(managementResourceInboundPort, nb, data_generator, mapReceiveTupleinboundPorturi, pluginId);
		
	}

	@Override
	public void createPluginMap(String managementMapInboundPort, int nb, Function<Tuple, Tuple> fonction_map,
			String mapReceiveTupleinboundPorturi, String ReduceReceiveTupleinboundPortUri, int pluginId)
			throws Exception {
		((createPluginI)this.getConnector()).createPluginMap(managementMapInboundPort, nb, fonction_map, mapReceiveTupleinboundPorturi, ReduceReceiveTupleinboundPortUri, pluginId);
		
	}

	@Override
	public void createPluginReduce(Tuple pluginInfo) throws Exception {
		((createPluginI)this.getConnector()).createPluginReduce(pluginInfo);
		
	}

/*	@Override
	public void createPluginReduce(String managementReduceInboundPort, int nb,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, String ReduceReceiveTupleinboundPorturi,
			String sendResultinboundPortUri, int pluginId) throws Exception {
		((createPluginI)this.getConnector()).createPluginReduce(managementReduceInboundPort, nb, fonction_reduce, ReduceReceiveTupleinboundPorturi, sendResultinboundPortUri, pluginId);
		
	}
	
	*/
	

	

}
