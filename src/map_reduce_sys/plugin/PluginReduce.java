package map_reduce_sys.plugin;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.swing.text.StyledEditorKit.BoldAction;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.ManagementInboundPortForPlugin;
import map_reduce_sys.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.reduce.ManagementReduceInboundPortForPlugin;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;


public class PluginReduce extends AbstractPlugin implements ManagementI,SendTupleServiceI{
	private static final long serialVersionUID=1L;
	
	//Offere the servive runTaskReduce
	protected ManagementInboundPortForPlugin managementReducePluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	protected  int dataSize;
	protected BiFunction<Tuple,Tuple, Tuple> fonction_reduce;
	protected BlockingQueue<OrderedTuple>  bufferReceive;
	protected int indexCalculExector;
	//protected int indexSendExector;
	//send tuple to  another component
	protected SendTupleOutboundPort sendTupleobp;
	protected ReceiveTupleWithPluginInboundPort ReceiveTupleInboundPort;
	protected String receiveTupleInPortUri;
	
	
	//uri of the inboundPort of another Component 
	// used to do the connection
	protected String sendReduceTupleInboundPortUri;

	

	
	public PluginReduce(String uri,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String inboundPortReceiveTupleuri,String inboundPortSendTupleUri) {

		super();
		
		this.ManagementInPortUri=uri;
		this.receiveTupleInPortUri=inboundPortReceiveTupleuri;
		this.sendReduceTupleInboundPortUri=inboundPortSendTupleUri;
		this.nbThread=nb;
		this.fonction_reduce=fonction_reduce;
		bufferReceive=new PriorityBlockingQueue<OrderedTuple>();
		
		
		
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
		this.managementReducePluginInboundPort = new ManagementInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementReducePluginInboundPort.publishPort();
		
		this.addOfferedInterface(SendTupleServiceI.class);
		
		this.ReceiveTupleInboundPort=new ReceiveTupleWithPluginInboundPort(receiveTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.ReceiveTupleInboundPort.publishPort();
		
		indexCalculExector=createNewExecutorService("ReduceCalculexector_uri", nbThread,false);
		
		
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
		
		this.ReceiveTupleInboundPort.unpublishPort();
		this.ReceiveTupleInboundPort.destroyPort();
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
	    	bufferReceive=new LinkedBlockingQueue<>();
	        
			for(int i=0;i<dataSize-1;i++) {
				OrderedTuple t1=(OrderedTuple) bufferReceive.take();
				OrderedTuple t2=(OrderedTuple) bufferReceive.take();
				
				this.getOwner().runTask(indexCalculExector,reduce -> {try {
					((createCalculServiceI)reduce).createReduceCalculTask((LinkedBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
				} catch (Exception e) {
					e.printStackTrace();
				}});
				
				
				
			}
				Tuple finalResult = bufferReceive.take();
				//int result = (int) finalResult.getIndiceData(0);
				
			//	System.out.println("final result is :  " + result);
				System.out.println("Component Reduce finished" );
				this.sendTupleobp.tupleSender(finalResult);
			return true;
	    	
	    }
	       
	    case ASSOCIATIVE :
	    {
	    	this.bufferReceive=new PriorityBlockingQueue<OrderedTuple>();
			OrderedTuple tmp1=null;
			OrderedTuple tmp2=null;
			
			for(int i=0;i<dataSize-1;i++) {
				boolean LastCoupleMatched=false;
				boolean fistEntre=true;
				while (!LastCoupleMatched) {
					if(fistEntre) {
						 tmp1=(OrderedTuple) bufferReceive.take();
						 tmp2=(OrderedTuple) bufferReceive.take();
					}else {
						tmp2=tmp1;
						tmp1=bufferReceive.take();
					}
					fistEntre=false;
					
					if((tmp1.getId()+1)==tmp2.getId()||(tmp1.getId()+1)==tmp2.getRangeMin()||
							(tmp2.getId()+1)==tmp1.getId()||(tmp2.getId()+1)==tmp1.getRangeMin()) {
							LastCoupleMatched=true;
						
					}else {
						bufferReceive.put(tmp2);
					}
						
						
				}
				OrderedTuple t1=tmp1;
				OrderedTuple t2=tmp2;
				
				if(t1.getId()<t2.getId()) {

					this.getOwner().runTask(indexCalculExector,reduce -> {try {
						((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
					} catch (Exception e) {
						e.printStackTrace();
					}});
					
				}else {

					this.getOwner().runTask(indexCalculExector,reduce -> {try {
						((createCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t2, t1);
					} catch (Exception e) {
						e.printStackTrace();
					}});
				}
				
				
				
			}
					
			OrderedTuple finalResult = bufferReceive.take();
			/*while (finalResult.getId()!=(dataSize-1)||finalResult.getRangeMin()!=0) {
				OrderedTuple tmp=finalResult;
				bufferReceive.put(tmp);
				finalResult=bufferReceive.take();
				
			}*/
			//int result = (int) finalResult.getIndiceData(0);
			System.out.println("final result id is :  " + finalResult.getId());
			System.out.println("Component Reduce finished" );
			this.sendTupleobp.tupleSender(finalResult);
			return true;
	    	
	       
	     
	       
	    }  
	    case ITERATIVE :{
	    	System.out.println("Component Rduce run task " + this.getOwner() );
            bufferReceive=new PriorityBlockingQueue<>();
			      
			        
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
							((createCalculServiceI)reduce).createReduceCalculTask((BlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
							
						} 
							catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}});
						
					
					
				}
			
					Tuple finalResult = bufferReceive.take();
					//int result = (int) finalResult.getIndiceData(0);
					//System.out.println("final result is :  " + result);
					System.out.println("Component Reduce finished" );
					this.sendTupleobp.tupleSender(finalResult);
				return true;
	    }
	       
	}
		
		return false;	
       
	}


	//receive tuple from component map and put the tuple to bufferReceive
	@Override
	public void tupleSender(Tuple t) throws Exception {
		bufferReceive.put((OrderedTuple) t);
		System.out.println("Component reduce receive  :" +((OrderedTuple) t).getId());
		
	}



	@Override
	public void DoPluginPortConnection() throws Exception {
		System.out.println("Component reduce send inbound  :" +sendReduceTupleInboundPortUri);
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}



	

	
	
}