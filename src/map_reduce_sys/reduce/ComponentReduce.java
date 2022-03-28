package map_reduce_sys.reduce;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.CalculServiceOutboundPort;
import map_reduce_sys.OrderedTuple;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;

@OfferedInterfaces(offered = { RecieveTupleServiceI.class })
public class ComponentReduce extends AbstractComponent {

	public static final String RRMIP_URI = "rrmip-uri";
	public static final String GRDIP_URI = "grdip-uri";
	protected BiFunction<Tuple, Tuple, Tuple> fonction_reduce;
	protected PriorityBlockingQueue<Tuple> bufferRecive;
	//protected LinkedBlockingQueue<Tuple> bufferSend;
	protected ThreadPoolExecutor calculExecutor;

	protected ReduceReciveMapInboundPort rrmip;
	protected GestionReduceInboundPort grdip;
	protected Tuple finalResult;
	protected int dataSize;

	protected ComponentReduce(/*BiFunction<Tuple, Tuple, Tuple> g*/) throws Exception {
		super(2, 0);
		//this.fonction_reduce = g;
		this.bufferRecive = new PriorityBlockingQueue<Tuple>(20);
		//this.bufferSend = new LinkedBlockingQueue<Tuple>(20);
		this.rrmip = new ReduceReciveMapInboundPort(RRMIP_URI, this);
		this.rrmip.publishPort();
		this.grdip=new GestionReduceInboundPort(GRDIP_URI,this);
		this.grdip.publishPort();
		int N = 2;
		calculExecutor = new ThreadPoolExecutor(N, N, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20));
		calculExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
	}

	@Override
	public synchronized void execute() throws Exception {
		super.execute();

		/*while (true) {
			Tuple t1 = bufferRecive.take();
			Tuple t2 = bufferRecive.take();
			if (t2.getIndiceData(0) instanceof Boolean || t1.getIndiceData(0) instanceof Boolean) {
				System.out.println("Calcul finished, the result is :" + t1.getIndiceData(0));
				//bufferSend.add(t1);
				finalResult=t1;
				break;
			}
			// application(t1,t2);
			Runnable taskCalcul = () -> {
				bufferRecive.addFirst(fonction_reduce.apply(t1,t2));
			};
			calculExecutor.submit(taskCalcul);

		}
		//Tuple tuple = bufferSend.take();
		int result = (int) finalResult.getIndiceData(0);
		
		System.out.println("final result is :  " + result);
		calculExecutor.shutdown();*/

	}
	
	
	
public boolean	runTaskReduce(BiFunction<Tuple,Tuple,Tuple>function,int size) throws Exception {
	this.fonction_reduce=function;
	this.dataSize=size;
	
	/*while (true) {
		Tuple t1 = bufferRecive.take();
		Tuple t2 = bufferRecive.take();
		if (t2.getIndiceData(0) instanceof Boolean || t1.getIndiceData(0) instanceof Boolean) {
			System.out.println("Calcul finished, the result is :" + t1.getIndiceData(0));
			//bufferSend.add(t1);
			finalResult=t1;
			break;
		}
		// application(t1,t2);
		Runnable taskCalcul = () -> {
			bufferRecive.addFirst(fonction_reduce.apply(t1,t2));
		};
		calculExecutor.submit(taskCalcul);

	}
	//Tuple tuple = bufferSend.take();
	int result = (int) finalResult.getIndiceData(0);
	
	System.out.println("final result is :  " + result);
	//calculExecutor.shutdown();*/
	
	for(int i=0;i<dataSize-1;i++) {
		OrderedTuple t1=(OrderedTuple) bufferRecive.take();
		OrderedTuple t2=(OrderedTuple) bufferRecive.take();
		
		Runnable taskCalcul = () -> {
			bufferRecive.add(fonction_reduce.apply(t1,t2));
			
		};
		calculExecutor.submit(taskCalcul);
	}
		Tuple tuple = bufferRecive.take();
		int result = (int) finalResult.getIndiceData(0);
		
		System.out.println("final result is :  " + result);
	
	return true;
}
	
	
	
	
	
	

	public Tuple send_Tuple() throws InterruptedException {

		//return bufferSend.take();
		return finalResult;

	}

	/*
	 * public void application(Tuple t1,Tuple t2) throws InterruptedException {
	 * 
	 * //Tuple t1=bufferRecive.take();
	 * 
	 * Double double1=0.0; Tuple t1=new Tuple(1); t1.setIndiceTuple(0, double1);
	 * 
	 * //Tuple t2=bufferRecive.take();
	 * bufferRecive.addFirst(fonction_reduce.apply(t1,t2));//add in the head }
	 */

	public boolean recieve_Tuple(Tuple t) throws InterruptedException {
		bufferRecive.put(t);// add in the tail of the deque
		System.out.println("Component reduce receive  :" + t.getIndiceData(0));
		return true;
	}

	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}

}
