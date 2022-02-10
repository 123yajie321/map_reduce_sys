package map_reduce_sys;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;

public class ComponentRessource extends AbstractComponent {
	
	protected Function<Void, Tuple> data_generator;
	protected ConcurrentLinkedQueue<Tuple> bufferSend;
	

	protected ComponentRessource(int nbThreads, int nbSchedulableThreads,Function<Void, Tuple> s) {
		super(nbThreads, nbSchedulableThreads);
		this.data_generator=s;
		
	}
	
	public void application() {
		
		bufferSend.add(this.data_generator.apply(null));
	}
	
   public Tuple send_Tuple() {
	   
	   return bufferSend.poll();
	   
   }

}
