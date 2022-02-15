package map_reduce_sys.reduce;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.CalculServiceOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
@OfferedInterfaces(offered= {RecieveTupleServiceI.class})
public class ComponentReduce extends AbstractComponent {
	
	public static final String RRMIP_URI = "rrmip-uri";
	protected BiFunction<Tuple, Tuple, Tuple> fonction_reduce;
	protected LinkedBlockingQueue<Tuple> bufferRecive;
	protected LinkedBlockingQueue<Tuple> bufferSend;
	
	protected ReduceReciveMapInboundPort rrmip;
	
	protected ComponentReduce(BiFunction<Tuple, Tuple, Tuple> g) throws Exception {
		super(1, 0);
		this.fonction_reduce = g;
		this.bufferRecive=new LinkedBlockingQueue<Tuple>();
		this.bufferSend=new LinkedBlockingQueue<Tuple>();
		this.rrmip=new ReduceReciveMapInboundPort(RRMIP_URI,this);
		this.rrmip.publishPort();
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		application();
		Tuple tuple=bufferSend.take();
		Double result=(Double)tuple.getIndiceData(0);
		System.out.println("final result is :  "+ result);
		
		
		
	}
	
	public Tuple send_Tuple() throws InterruptedException {
		   
		   return bufferSend.take();
		   
	   }
	
	public boolean Recieve_Tuple(Tuple t)throws InterruptedException{
		   
		   bufferRecive.put(t);
		   return true;
	   }
	
	public void application() throws InterruptedException {
		
		//Tuple t1=bufferRecive.take();
		Double double1=0.0;
		Tuple t1=new Tuple(1);
		t1.setIndiceTuple(0, double1);
		Tuple t2=bufferRecive.take();
		bufferSend.put(fonction_reduce.apply(t1,t2));
	}

	public boolean recieveTuple(Tuple t) throws InterruptedException {
		bufferRecive.put(t);
		return true;
	}
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}
	

}
