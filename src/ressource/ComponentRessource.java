package ressource;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.Tuple;

public class ComponentRessource extends AbstractComponent {
	public static final String RSMOP_URI = "rsmop-uri";
	protected  RessourceSendMapOutboundPort rsmop;
	protected Function<Void, Tuple> data_generator;
	
	protected LinkedBlockingQueue<Tuple> bufferSend;
	
	protected ComponentRessource(Function<Void, Tuple> s) throws Exception {
		super(1, 0);
		this.data_generator=s;
		this.rsmop=new RessourceSendMapOutboundPort(RSMOP_URI,this);
		this.rsmop.publishPort();
		bufferSend=new LinkedBlockingQueue<Tuple>();
	}
	
	
	public void application() {
	
		Void v = null;
		bufferSend.add(data_generator.apply(v));
	}
	
	
	@Override
	public synchronized void finalise() throws Exception {		
		this.doPortDisconnection(RSMOP_URI);
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}
	
	
	
}
