package ressource;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.SendTupleServiceI;
@RequiredInterfaces(required ={SendTupleServiceI.class})
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
	
	
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		for(int i=0;i<10;i++) {
			application();
			Tuple t = bufferSend.take();
			this.rsmop.tupleSender(t);
			System.out.println("Component  create ressource :" +t.getIndiceData(0));
		}
		Tuple fin= new Tuple(1);
		fin.setIndiceTuple(0, true);
		bufferSend.add(fin);
		this.rsmop.tupleSender(fin);
		System.out.println("Component Ressource  finished :");
		
		
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
