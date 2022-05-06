package map_reduce_sys.connector;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public class ConnectorGestion extends AbstractConnector implements ManagementI{

	

	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		 return ((ManagementI)this.offering).runTaskResource(function,t);
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return ((ManagementI)this.offering).runTaskMap(function,t);
	
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		return ((ManagementI)this.offering).runTaskReduce(function, t,nature);
	}

	@Override
	public void DoPluginPortConnection() throws Exception {
		((ManagementI)this.offering).DoPluginPortConnection();
		
	}

	/*@Override
	public void createPluginResource(String managementResourceInboundPort, int nb,
			Function<Integer, Tuple> data_generator, String mapReceiveTupleinboundPorturi, int pluginId)
			throws Exception {
		((ManagementI)this.offering).createPluginResource(managementResourceInboundPort, nb, data_generator, mapReceiveTupleinboundPorturi, pluginId);
		
	}

	@Override
	public void createPluginMap(String managementMapInboundPort, int nb, Function<Tuple, Tuple> fonction_map,
			String mapReceiveTupleinboundPorturi, String ReduceReceiveTupleinboundPortUri, int pluginId)
			throws Exception {
		((ManagementI)this.offering).createPluginMap(managementMapInboundPort, nb, fonction_map, mapReceiveTupleinboundPorturi, ReduceReceiveTupleinboundPortUri, pluginId);
		
	}

	@Override
	public void createPluginReduce(String managementReduceInboundPort, int nb,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, String ReduceReceiveTupleinboundPorturi,
			String sendResultinboundPortUri, int pluginId) throws Exception {
		((ManagementI)this.offering).createPluginReduce(managementReduceInboundPort, nb, fonction_reduce, ReduceReceiveTupleinboundPorturi, sendResultinboundPortUri, pluginId);
		
	}*/



	
	

}
