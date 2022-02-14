package map_reduce_sys.map;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
@OfferedInterfaces(offered= {RecieveTupleServiceI.class})
@RequiredInterfaces(required ={SendTupleServiceI.class})
public class ComponentMap extends AbstractComponent {

	public static final String MSROP_URI = "msrop-uri";
	public static final String MSOP_URI = "msop-uri";
	public static final String MGIP_URI = "mgip-uri";
	

	protected MapSendReduceOutboundPort msrop;
	protected MapServiceOutboundPort msop;// pour envouyer Tuple a Maps
	protected MapGestionInboundPort mgip;//pour recevoir Tuple depuis Map

	
	protected Function<Tuple, Tuple> fonction_map;
	protected ConcurrentLinkedQueue<Tuple> bufferRecive;
	protected ConcurrentLinkedQueue<Tuple> bufferSend;
	
	protected ComponentMap(Function<Tuple, Tuple> f) throws Exception {
		super(1,0);
		/*
		 * this.msop = new MapServiceOutboundPort(MSOP_URI,this);
		 * this.msop.publishPort();
		 */		
		this.msrop=new MapSendReduceOutboundPort(MSROP_URI,this);
		this.mgip = new MapGestionInboundPort(MGIP_URI,this);
		this.msrop.publishPort();
		this.mgip.publishPort();
		
		
		this.fonction_map=f;
		this.bufferRecive=new ConcurrentLinkedQueue<Tuple>();
		this.bufferSend=new ConcurrentLinkedQueue<Tuple>();
		
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		while(true) {
			application();
			Tuple t = bufferSend.poll();
			this.msrop.tupleSender(t);
		}
		
		
	}
	
	public void send_Tuple(Tuple t) {
		   
		   //return bufferSend.poll();
		   
	   }
	
	public boolean recieve_Tuple(Tuple t) {
		   this.bufferRecive.add(t);
		   return true;
	   }
	
	public void application() {
		Tuple t=bufferRecive.poll();
		bufferSend.add(fonction_map.apply(t));
	}
	
	
	
	@Override
	public synchronized void finalise() throws Exception {		
		this.doPortDisconnection(MSROP_URI);
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}
	
	
	

}
