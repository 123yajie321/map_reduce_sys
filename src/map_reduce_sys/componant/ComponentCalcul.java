package map_reduce_sys.componant;

import java.util.concurrent.BlockingQueue;
import fr.sorbonne_u.components.AbstractComponent;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.CreateCalculServiceI;
import map_reduce_sys.ports.SendTupleOutboundPort;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;
/**
 * The class <code>ComponentCalcul</code> implements a component that can
 *  perform some computational tasks
 * 
 *  <p>
 * The component implements the {@code SendTupleImplementationI} interface
 * to send tuple to other component and it implements also l'interface  
 * {@code CreateCalculServiceI} to execute calculate tasks
 * </p>
 *    
 *    
 * @author Yajie LIU, Zimeng ZHANG
 */




public class ComponentCalcul extends AbstractComponent implements SendTupleImplementationI,CreateCalculServiceI {

	protected ComponentCalcul(String uri) throws Exception {
		super(uri,3, 0);
		
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		
	}
	
	@Override
	public void createResourceCalculTask(BlockingQueue<Tuple>bufferSend,Function<Integer, Tuple> function,int tupleId) {
		
		OrderedTuple t1=(OrderedTuple) function.apply(tupleId);
		bufferSend.add(t1);
	}
	
	
	@Override
	public void createMapCalculTask(BlockingQueue<Tuple>bufferSend,Function<Tuple, Tuple> fonction_map,Tuple t) throws Exception{
		bufferSend.add(fonction_map.apply(t));	
	}
	
	
	@Override
	public void createReduceCalculTask(BlockingQueue<OrderedTuple> bufferReceive,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, Tuple t1, Tuple t2) throws Exception {
		OrderedTuple result= (OrderedTuple) fonction_reduce.apply(t1,t2);
		bufferReceive.put(result);	
	}
	

	public void send_Tuple(SendTupleOutboundPort port, Tuple result) throws Exception {
		port.tupleSender(result);
		System.out.println("Component Send  tuple id: "+((OrderedTuple) result).getId()); 
		  
	   }

	
	
}
