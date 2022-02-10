package map_reduce_sys;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;

@RequiredInterfaces(required ={SendTupleServiceI.class})
public class ComponentMap extends AbstractComponent {

	
	protected Function<Tuple, Tuple> fonction_map;
	protected ConcurrentLinkedQueue<Tuple> bufferRecive;
	protected ConcurrentLinkedQueue<Tuple> bufferSend;
	
	protected ComponentMap(int nbThreads, int nbSchedulableThreads,Function<Tuple, Tuple> f) {
		super(nbThreads, nbSchedulableThreads);
		this.fonction_map=f;
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
		Tuple t=bufferRecive.poll();
		bufferSend.add(fonction_map.apply(t));
	}
	
	
	
	
	
	

}
