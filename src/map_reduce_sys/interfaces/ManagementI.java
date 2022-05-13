package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;

import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public interface ManagementI  extends OfferedCI,RequiredCI{
	/*
	public <T,R> boolean SetFunction(Function<T, R> f);
	public <T,T2,R> boolean SetBiFunction(BiFunction<T,T2,R> f);
	public void SubmitJob(SimpleJob job);*/
	
	public boolean runTaskResource(Function<Integer, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskMap(Function<Tuple, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskReduce(BiFunction<Tuple,Tuple, Tuple>function,Tuple t,Nature nature) throws Exception;
	public void DoPluginPortConnection()throws Exception;
	
	/*
	 * public void createPluginResource(String managementResourceInboundPort,int
	 * nb,Function<Integer, Tuple> data_generator,String
	 * mapReceiveTupleinboundPorturi,int pluginId) throws Exception; public void
	 * createPluginMap(String managementMapInboundPort,int nb,Function<Tuple, Tuple>
	 * fonction_map,String mapReceiveTupleinboundPorturi,String
	 * ReduceReceiveTupleinboundPortUri,int pluginId) throws Exception ; public void
	 * createPluginReduce(String managementReduceInboundPort,int
	 * nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String
	 * ReduceReceiveTupleinboundPorturi,String sendResultinboundPortUri,int
	 * pluginId) throws Exception;
	 */
	
	
}
