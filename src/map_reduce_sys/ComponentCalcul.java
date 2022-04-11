package map_reduce_sys;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.OrderedTuple;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.ressource.GestionResourceInboundPort;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;
@RequiredInterfaces(required ={SendTupleServiceI.class})
public class ComponentCalcul extends AbstractComponent implements SendTupleServiceI,ManagementI {
	

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
	
	public void createReduceCalculTask(BlockingQueue<Tuple>bufferReceive,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,Tuple t1,Tuple t2){
		bufferReceive.add(fonction_reduce.apply(t1,t2));	
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	
	
}