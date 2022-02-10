package map_reduce_sys;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;

public class ComponentReduce extends AbstractComponent {
	
	
	protected BiFunction<Tuple, Tuple, Tuple> fonction_reduce;
	protected ConcurrentLinkedQueue<Tuple> bufferRecive;
	protected ConcurrentLinkedQueue<Tuple> bufferSend;
	
	protected ComponentReduce(int nbThreads, int nbSchedulableThreads,BiFunction<Tuple, Tuple, Tuple> g) {
		super(nbThreads, nbSchedulableThreads);
		this.fonction_reduce = g;
		this.bufferRecive=new ConcurrentLinkedQueue<Tuple>();
		this.bufferSend=new ConcurrentLinkedQueue<Tuple>();
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		
	}
	
	public Tuple send_Tuple() {
		   
		   return bufferSend.poll();
		   
	   }
	
	public boolean Recieve_Tuple(Tuple t) {
		   
		   return bufferRecive.add(t);
		   
	   }
	
	public void application() {
		
		Tuple t1=bufferRecive.poll();
		Tuple t2=bufferRecive.poll();

		bufferSend.add(fonction_reduce.apply(t1,t2));
	}
	

}
