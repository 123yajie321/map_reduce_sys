package map_reduce_sys.ressource;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.OrderedTuple;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.SendTupleServiceI;
@RequiredInterfaces(required ={SendTupleServiceI.class})
public class ComponentRessource extends AbstractComponent {
	public static final String RSMOP_URI = "rsmop-uri";
	public static final String GRSIP_URI = "grsip-uri";
	protected GestionResourceInboundPort grsip;
	protected  RessourceSendMapOutboundPort rsmop;
	protected Function<Void, Tuple> data_generator;
	protected ThreadPoolExecutor calculExecutor;
	protected ThreadPoolExecutor SendExecutor;
	protected LinkedBlockingQueue<Tuple> bufferSend;
	protected  int resourceSize;

	protected ComponentRessource(/*Function<Void, Tuple> s,int size*/) throws Exception {
		super(2, 0);
		//this.data_generator=s;
		this.rsmop=new RessourceSendMapOutboundPort(RSMOP_URI,this);
		this.grsip=new GestionResourceInboundPort(GRSIP_URI, this);
		this.rsmop.publishPort();
		this.grsip.publishPort();
		bufferSend=new LinkedBlockingQueue<Tuple>();
		int N=2;
		calculExecutor=new ThreadPoolExecutor(N, N, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20));
		calculExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		SendExecutor=new ThreadPoolExecutor(N, N, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20));
		SendExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		//this.resourceSize=size;
		
	}
	
	
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		System.out.println("Component Ressource executing");
		/*
		 * for(int i=0;i<resourceSize;i++) { application(); Tuple t = bufferSend.take();
		 * System.out.println("Component Ressource created ressource :"+t.getIndiceData(
		 * 0)); this.rsmop.tupleSender(t);
		 * System.out.println("Component  ressource send " +t.getIndiceData(0)); } Tuple
		 * fin= new Tuple(1); fin.setIndiceTuple(0, true); bufferSend.add(fin);
		 * this.rsmop.tupleSender(fin);
		 * System.out.println("Component Ressource  finished :");
		 */
		
		/*for(int i=0;i<resourceSize;i++) {
			Runnable taskCalcul=()->{
				Void v = null;
				Tuple t=data_generator.apply(v);
				bufferSend.add(t);
				System.out.println("Component Ressource created ressource :"+t.getIndiceData( 0)); 
				
			};		
			calculExecutor.submit(taskCalcul);
			Tuple result =bufferSend.take();
			Runnable task=()->{
				try {
					send_Tuple(result);
					System.out.println("Component Ressource Send ressource :"+result.getIndiceData( 0)); 
				} catch (Exception e) {
					e.printStackTrace();
					
				}
			};
			SendExecutor.submit(task);
			
		}
		
	
		Tuple fin= new Tuple(1); 
		fin.setIndiceTuple(0, true); 
		
		Runnable task=()->{
			try {
				send_Tuple(fin);
				System.out.println("Component Ressource Send ressource :"+fin.getIndiceData( 0)); 
			} catch (Exception e) {
				e.printStackTrace();
				
			}
		};
		SendExecutor.submit(task);
		
		calculExecutor.shutdown();
		SendExecutor.shutdown();
		System.out.println("Component resource finished" );*/
		
	}
	
	
	
	/*
	 * public void application() {
	 * 
	 * Void v = null; bufferSend.add(data_generator.apply(v)); }
	 */
	
	
	public boolean runTask(Function<Void, Tuple> s,int size) throws Exception {
		this.resourceSize=size;
		this.data_generator=s;
		for(int i=0;i<resourceSize;i++) {
			Runnable taskCalcul=()->{
				Void v = null;
				OrderedTuple t=(OrderedTuple) data_generator.apply(v);
				bufferSend.add(t);
				System.out.println("Component Ressource created ressource :"+t.getIndiceData( 0)); 
				
			};		
			calculExecutor.submit(taskCalcul);
			Tuple result =bufferSend.take();
			Runnable task=()->{
				try {
					send_Tuple(result);
					System.out.println("Component Ressource Send ressource :"+result.getIndiceData( 0)); 
				} catch (Exception e) {
					e.printStackTrace();
					
				}
			};
			SendExecutor.submit(task);
			
		}
		//Thread.sleep(100);
	
		/*Tuple fin= new Tuple(1); 
		fin.setIndiceTuple(0, true); 
		
		Runnable task=()->{
			try {
				send_Tuple(fin);
				System.out.println("Component Ressource Send ressource :"+fin.getIndiceData( 0)); 
			} catch (Exception e) {
				e.printStackTrace();
				
			}
		};
		SendExecutor.submit(task);*/
		
		
		calculExecutor.shutdown();
		SendExecutor.shutdown();
		System.out.println("Component resource finished" );
		
		
		
		
		return true;
	}
	
	
	
	
	public void send_Tuple(Tuple t) throws Exception {
		   
		this.rsmop.tupleSender(t);
		   
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
