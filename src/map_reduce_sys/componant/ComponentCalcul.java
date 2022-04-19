package map_reduce_sys.componant;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.ressource.GestionResourceInboundPort;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;

public class ComponentCalcul extends AbstractComponent implements SendTupleImplementationI,createCalculServiceI {
	

	protected ComponentCalcul(String uri) throws Exception {
		super(uri,2, 0);
		
		
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
		super.shutdown();
	}
	
	public void createResourceCalculTask(BlockingQueue<Tuple>bufferSend,Function<Void, Tuple> function) {
		
		Void v = null;
		OrderedTuple t1=(OrderedTuple) function.apply(v);
		bufferSend.add(t1);
	

}
	
	public void send_Tuple(SendTupleOutboundPort port, Tuple result) throws Exception {
		   
		port.tupleSender(result);
		System.out.println("Component Send  :"+result.getIndiceData( 0)+" id:"+((OrderedTuple) result).getId()); 
		   
	   }
	
	public void createMapCalculTask(BlockingQueue<Tuple>bufferSend,Function<Tuple, Tuple> fonction_map,Tuple t){
		bufferSend.add(fonction_map.apply(t));	
	}
	
	
	@Override
	public void createReduceCalculTask(PriorityBlockingQueue<OrderedTuple> bufferReceive,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, Tuple t1, Tuple t2) throws Exception {
		OrderedTuple result= (OrderedTuple) fonction_reduce.apply(t1,t2);
		bufferReceive.put(result);	
		System.out.println("result id"+result.getId()+" value"+ result.getIndiceData(0));
	}

	
	
}
