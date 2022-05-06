package map_reduce_sys.connector;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createPluginI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public class ConnectorCreatePlugin extends AbstractConnector implements createPluginI{

	
	@Override
	public void createPluginResource(String managementResourceInboundPort, int nb,
			Function<Integer, Tuple> data_generator, String mapReceiveTupleinboundPorturi, int pluginId)
			throws Exception {
		((createPluginI)this.offering).createPluginResource(managementResourceInboundPort, nb, data_generator, mapReceiveTupleinboundPorturi, pluginId);
		
	}

	@Override
	public void createPluginMap(String managementMapInboundPort, int nb, Function<Tuple, Tuple> fonction_map,
			String mapReceiveTupleinboundPorturi, String ReduceReceiveTupleinboundPortUri, int pluginId)
			throws Exception {
		((createPluginI)this.offering).createPluginMap(managementMapInboundPort, nb, fonction_map, mapReceiveTupleinboundPorturi, ReduceReceiveTupleinboundPortUri, pluginId);
		
	}

	@Override
	public void createPluginReduce(/*String managementReduceInboundPort, int nb,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, String ReduceReceiveTupleinboundPorturi,
			String sendResultinboundPortUri, int pluginId*/Tuple pluginInfo) throws Exception {
		//((createPluginI)this.offering).createPluginReduce(managementReduceInboundPort, nb, fonction_reduce, ReduceReceiveTupleinboundPorturi, sendResultinboundPortUri, pluginId);
		((createPluginI)this.offering).createPluginReduce(pluginInfo);
	}
	

}
