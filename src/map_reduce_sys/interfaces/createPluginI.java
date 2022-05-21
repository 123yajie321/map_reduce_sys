package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.structure.Tuple;

public interface createPluginI extends OfferedCI,RequiredCI {
	public void createPluginResource(String managementResourceInboundPort,int nb,Function<Integer, Tuple> data_generator,String mapReceiveTupleinboundPorturi,int pluginId) throws Exception;
	public void createPluginMap(String managementMapInboundPort,int nb,Function<Tuple, Tuple> fonction_map,String mapReceiveTupleinboundPorturi,String ReduceReceiveTupleinboundPortUri,int pluginId) throws Exception ;
	public void createPluginReduce(String managementReduceInboundPort,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String ReduceReceiveTupleinboundPorturi,String sendResultinboundPortUri,int pluginId) throws Exception;
}
