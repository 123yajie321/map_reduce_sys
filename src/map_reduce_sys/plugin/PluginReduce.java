package map_reduce_sys.plugin;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent.ExecutorServiceFactory;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.componant.ComponentCalcul;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.map.ManagementMapInboundPortForPlugin;
import map_reduce_sys.reduce.ManagementReduceInboundPortForPlugin;
import map_reduce_sys.ressource.ManagementResourceInboundPortForPlugin;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;


public class PluginReduce extends AbstractPlugin implements ManagementI,SendTupleServiceI{
	private static final long serialVersionUID=1L;
	
	//Offere the servive runTaskReduce
	protected ManagementReduceInboundPortForPlugin managementReducePluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	protected  int dataSize;
	protected BiFunction<Tuple,Tuple, Tuple> fonction_reduce;
	protected BlockingQueue<OrderedTuple>  bufferReceive;
	protected int indexCalculExector;
	protected int indexSendExector;
	//send tuple to  another component
	protected SendTupleOutboundPort sendTupleobp;
	
	
	protected ArrayList<String>receiveTupleInPortUriList;
	protected ArrayList<ReceiveTupleWithPluginInboundPort>receiveTupleInPortList;
	//recevive the tuple from component map
	//protected ReceiveTupleWithPluginInboundPort receiveTupleInboundPort;
	//protected ReceiveTupleWithPluginInboundPort receiveTupleInboundPort2;
	//uri of the inboundPort to receive tuple 
	//protected String receiveTupleInPortUri;
	//protected String receiveTupleInPortUri2;
	
	//uri of the inboundPort of another Component 
	// used to do the connection
	protected String sendReduceTupleInboundPortUri;

	

	
	public PluginReduce(/*String uri,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String inboundPortReceiveTupleuri,String inboundPortSendTupleUri*/Tuple pluginInfo) {

		super();
		
		/*this.ManagementInPortUri=uri;
		this.receiveTupleInPortUri=inboundPortReceiveTupleuri;
		this.sendReduceTupleInboundPortUri=inboundPortSendTupleUri;
		this.nbThread=nb;
		this.fonction_reduce=fonction_reduce;
		bufferReceive=new PriorityBlockingQueue<OrderedTuple>();*/
		
		this.ManagementInPortUri=(String) pluginInfo.getIndiceData(0);
		//this.receiveTupleInPortUri=(String) pluginInfo.getIndiceData(1);
		
		this.nbThread=(int) pluginInfo.getIndiceData(1);
		this.fonction_reduce=(BiFunction<Tuple, Tuple, Tuple>) pluginInfo.getIndiceData(2);
		
		this.receiveTupleInPortUriList=(ArrayList<String>) pluginInfo.getIndiceData(3);
		this.sendReduceTupleInboundPortUri=(String) pluginInfo.getIndiceData(4);
		
		
		
		
		
		//this.receiveTupleInPortUri2=(String) pluginInfo.getIndiceData(5);
		
		bufferReceive=new PriorityBlockingQueue<OrderedTuple>();
		this.receiveTupleInPortList=new ArrayList<ReceiveTupleWithPluginInboundPort>();
		
		
	}
	
	
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);

		this.addRequiredInterface(SendTupleServiceI.class);
		this.sendTupleobp=new SendTupleOutboundPort(this.getOwner());
		this.sendTupleobp.publishPort();
		
		
		
	}
	
	@Override
	public void initialise() throws Exception{
		//this.addRequiredInterface(ReflectionCI.class);
		//ReflectionOutboundPort  rop= new ReflectionOutboundPort(this.getOwner());
		super.initialise();
		
		
		//connecte with the component reduce to send Tuple
		
		this.addOfferedInterface(ManagementI.class);
		this.managementReducePluginInboundPort = new ManagementReduceInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementReducePluginInboundPort.publishPort();
		
		this.addOfferedInterface(SendTupleServiceI.class);
		/*this.receiveTupleInboundPort=new ReceiveTupleWithPluginInboundPort(receiveTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.receiveTupleInboundPort.publishPort();
		this.receiveTupleInboundPort2=new ReceiveTupleWithPluginInboundPort(receiveTupleInPortUri2,this.getPluginURI(),this.getOwner() );
		this.receiveTupleInboundPort2.publishPort();*/
		
		for(String uri:receiveTupleInPortUriList) {
			System.out.println("reduce receive in port "+uri);
			ReceiveTupleWithPluginInboundPort inboundPort=new ReceiveTupleWithPluginInboundPort(uri,this.getPluginURI(),this.getOwner() ); 
			inboundPort.publishPort();
			receiveTupleInPortList.add(inboundPort);
			
		}
		
		
		//this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
		
		indexCalculExector=createNewExecutorService("ReduceCalculexector_uri", nbThread,false);
		indexSendExector=createNewExecutorService("MapSendexector_uri", nbThread,false);
		
}
	
	@Override
	public void			finalise() throws Exception
	{
		this.getOwner().doPortDisconnection(this.sendTupleobp.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.managementReducePluginInboundPort.unpublishPort();
		this.managementReducePluginInboundPort.destroyPort();
		this.removeOfferedInterface(ManagementI.class);
		
		for(ReceiveTupleWithPluginInboundPort port:receiveTupleInPortList) {
			port.unpublishPort();
			port.destroyPort();
		}
//		
//		this.receiveTupleInboundPort.unpublishPort();
//		this.receiveTupleInboundPort.destroyPort();
//		this.receiveTupleInboundPort2.unpublishPort();
//		this.receiveTupleInboundPort2.destroyPort();
		this.removeOfferedInterface(SendTupleServiceI.class);
		
		this.sendTupleobp.unpublishPort();
		this.sendTupleobp.destroyPort();
		this.removeRequiredInterface(SendTupleServiceI.class);
		
		
	}



	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}



	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
		
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {

		this.dataSize=(int) t.getIndiceData(0)-(int)t.getIndiceData(1);
		this.fonction_reduce=function;
		int currentCalculId=(int) t.getIndiceData(1);
		
		
		
		switch(nature){
	    case COMMUTATIVE_ASSOCIATIVE :
	    {
	    	
	        
			for(int i=0;i<dataSize-1;i++) {
				OrderedTuple t1=(OrderedTuple) bufferReceive.take();
				OrderedTuple t2=(OrderedTuple) bufferReceive.take();
				
				this.getOwner().runTask(indexCalculExector,reduce -> {try {
					((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
				} catch (Exception e) {
					e.printStackTrace();
				}});
				
				
				
			}
				Tuple finalResult = bufferReceive.take();
				int result = (int) finalResult.getIndiceData(0);
				
				System.out.println("final result is :  " + result);
				System.out.println("Component Reduce finished" );
				this.sendTupleobp.tupleSender(finalResult);
			return true;
	    	
	    }
	       
	    case ASSOCIATIVE :
	    {
	    	this.bufferReceive=new LinkedBlockingQueue<>();
			OrderedTuple tmp1=null;
			OrderedTuple tmp2=null;
			for(int i=0;i<dataSize-1;i++) {
				
				if(i==0) {
					 tmp1=(OrderedTuple) bufferReceive.take();
					 tmp2=(OrderedTuple) bufferReceive.take();
				}else {
					tmp1=tmp2;
					tmp2=bufferReceive.take();
				}
				
				OrderedTuple t1=tmp1;
				OrderedTuple t2=tmp2;
						
				if(t1.getId()<t2.getId()) {
						if((t1.getId()+1)==t2.getId()||(t1.getId()+1)==t2.getRangeMin()) {
								this.getOwner().runTask(indexCalculExector,reduce -> {try {
									((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}});
								
						}else {
								bufferReceive.put(t1);
								continue;
						}
					
				}else {
					
						if((t2.getId()+1)==t1.getId()||(t2.getId()+1)==t1.getRangeMin()) {
							this.getOwner().runTask(indexCalculExector,reduce -> {try {
								((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t2, t1);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}});
							
						}else {
								bufferReceive.put(t1);
								continue;
						}
					
				}
				
				
				
			}
				Tuple finalResult = bufferReceive.take();
				int result = (int) finalResult.getIndiceData(0);
				
				System.out.println("final result is :  " + result);
				System.out.println("Component Reduce finished" );
				this.sendTupleobp.tupleSender(finalResult);
			return true;
	    	
	       
	     
	       
	    }  
	    case ITERATIVE :{
	    	System.out.println("Component Rduce run task " + this.getOwner() );
		    
			      
			        
					for(int i=0;i<dataSize-1;i++) {
						OrderedTuple tmp1=(OrderedTuple) bufferReceive.take();
						while(currentCalculId!=tmp1.getId()) {
							
							bufferReceive.put(tmp1);
							tmp1=(OrderedTuple) bufferReceive.take();
						}
						currentCalculId++;
						OrderedTuple t1=tmp1;
					
						OrderedTuple tmp2=(OrderedTuple) bufferReceive.take();
						while(currentCalculId!=tmp2.getId()) {
							bufferReceive.put(tmp2);
							tmp2=(OrderedTuple) bufferReceive.take();
							
						}
						OrderedTuple t2=tmp2;
						this.getOwner().runTask(indexCalculExector,reduce -> {
							try {
							((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
							
						} 
							catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}});
						
					
					
				}
			
					Tuple finalResult = bufferReceive.take();
					int result = (int) finalResult.getIndiceData(0);
					System.out.println("final result is :  " + result);
					System.out.println("Component Reduce finished" );
					this.sendTupleobp.tupleSender(finalResult);
				return true;
	    }
	       
	}
		
		return false;	
       
	}


	//receive tuple from component map and put the tuple to bufferReceive
	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		bufferReceive.put((OrderedTuple) t);
		System.out.println("Component reduce receive  :" +((OrderedTuple) t).getId());
		return true;
		
		
	}



	@Override
	public void DoPluginPortConnection() throws Exception {
		System.out.println("Component reduce send inbound  :" +sendReduceTupleInboundPortUri);
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}



	

	
	
}