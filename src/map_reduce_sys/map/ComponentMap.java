package map_reduce_sys.map;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.SendTupleServiceI;

@RequiredInterfaces(required ={SendTupleServiceI.class})
public class ComponentMap extends AbstractComponent {

	
	public static final String MSOP_URI = "msop-uri";
	public static final String MGIP_URI = "mgip-uri";

	
	protected MapServiceOutboundPort msop;
	protected MapGestionInboundPort mgip;

	
	protected Function<Tuple, Tuple> fonction_map;
	protected ConcurrentLinkedQueue<Tuple> bufferRecive;
	protected ConcurrentLinkedQueue<Tuple> bufferSend;
	
	protected ComponentMap(Function<Tuple, Tuple> f) throws Exception {
		super(1,0);
		this.msop = new MapServiceOutboundPort(MSOP_URI,this);
		this.msop.publishPort();
		
		this.mgip = new MapGestionInboundPort(MGIP_URI,this);
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
			this.msop.tupleSender(t);
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
	
	
	
	
	
	

}
