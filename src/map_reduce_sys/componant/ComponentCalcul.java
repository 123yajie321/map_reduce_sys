package map_reduce_sys.componant;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.interfaces.createPluginI;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.plugin.PluginResource;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;
@OfferedInterfaces(offered ={createPluginI.class})
public class ComponentCalcul extends AbstractComponent implements SendTupleImplementationI,createCalculServiceI {
	protected CreatePluginInboundPort cpip;

	protected ComponentCalcul(String uri) throws Exception {
		super(2, 0);
		cpip=new CreatePluginInboundPort(uri,this);
		cpip.publishPort();
		
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		
	}
	
	
	@Override
	public synchronized void finalise() throws Exception {		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		try {
			cpip.unpublishPort();
		} catch (Exception e) {
		
			e.printStackTrace();
		}
		super.shutdown();
	}
	
	@Override
	public void createResourceCalculTask(BlockingQueue<Tuple>bufferSend,Function<Integer, Tuple> function,int tupleId) {
		
		OrderedTuple t1=(OrderedTuple) function.apply(tupleId);
		bufferSend.add(t1);
	

}
	
	
	@Override
	public void createMapCalculTask(BlockingQueue<Tuple>bufferSend,Function<Tuple, Tuple> fonction_map,Tuple t) throws Exception{
		bufferSend.add(fonction_map.apply(t));	
		//Thread.sleep(1L);
	}
	
	
	@Override
	public void createReduceCalculTask(BlockingQueue<OrderedTuple> bufferReceive,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, Tuple t1, Tuple t2) throws Exception {
		OrderedTuple result= (OrderedTuple) fonction_reduce.apply(t1,t2);
		System.out.println("result id"+result.getId()+" min range !!"+ result.getRangeMin());
		bufferReceive.put(result);	
		
		//System.out.println("result id :"+result.getId());
		//Thread.sleep(1L);
	}
	

	public void send_Tuple(SendTupleOutboundPort port, Tuple result) throws Exception {
		   
		port.tupleSender(result);
		//System.out.println("Component Send  :"+result.getIndiceData( 0)+" id:"+((OrderedTuple) result).getId()); 
		System.out.println("Component Send  tuple id: "+((OrderedTuple) result).getId()); 
		  
	   }

	public void createPluginResource(String managementResourceInboundPort,int nb,Function<Integer, Tuple> data_generator,String mapReceiveTupleinboundPorturi,int pluginId) throws Exception {
		PluginResource pluginResourceIn=new PluginResource(managementResourceInboundPort, nb,data_generator,mapReceiveTupleinboundPorturi);
		pluginResourceIn.setPluginURI("PluginResourceIn"+pluginId);
		this.installPlugin(pluginResourceIn);
		System.out.println("Component res plugin installed"); 
		
	}
	
	
	public void createPluginMap(String managementMapInboundPort,int nb,Function<Tuple, Tuple> fonction_map,String mapReceiveTupleinboundPorturi,String ReduceReceiveTupleinboundPortUri,int pluginId) throws Exception {
		PluginMap pluginMapIn=new PluginMap(managementMapInboundPort, nb,fonction_map,mapReceiveTupleinboundPorturi,ReduceReceiveTupleinboundPortUri);
		pluginMapIn.setPluginURI("pluginMapIn"+pluginId);
		this.installPlugin(pluginMapIn);
		System.out.println("Component map plugin installed"); 
	}
	
	
	public void createPluginReduce(/*String managementReduceInboundPort,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String ReduceReceiveTupleinboundPorturi,String sendResultinboundPortUri,int pluginId*/ Tuple pluginInfo) throws Exception {
		
		PluginReduce pluginReduceIn=new PluginReduce(pluginInfo);
		pluginReduceIn.setPluginURI("pluginReduceIn"+pluginInfo.getIndiceData(pluginInfo.getDimension()-1));
		this.installPlugin(pluginReduceIn);
		System.out.println("Component reduce plugin installed"); 
		
	}
	
	 /*public  static long generateRandomNumber(){

		 Random random = new Random();
		 float pointeur = 0;
		 long randomNumber;

		 pointeur = random.nextFloat();

		 if (pointeur < 0.5) {

		 randomNumber = 0L+ (random.nextLong() * (5L - 0L));

		 } else {

			 randomNumber = 5L + (random.nextLong() * (10L - 5L));

		 }

		 	System.out.println(randomNumber);
		 	return randomNumber;

		 }
	 */

	
	
}
