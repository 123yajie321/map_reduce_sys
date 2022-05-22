package map_reduce_sys.plugin;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.swing.text.StyledEditorKit.BoldAction;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementCI;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.interfaces.CreateCalculServiceI;
import map_reduce_sys.ports.ManagementInboundPortForPlugin;
import map_reduce_sys.ports.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.ports.SendTupleOutboundPort;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>PluginReduce</code> implements the calculate component side  plug-in
 * for the <code>ManagementCI</code> component interface and it implements also the the client side  and server side 
 * for the <code>SendTupleServiceCI</code> component interface
 * @author Yajie LIU, Zimeng ZHANG
 */
public class PluginReduce extends AbstractPlugin implements ManagementCI{
	private static final long serialVersionUID=1L;
	
	/**Inbound port for management, Offer the service runTaskReduce*/
	protected ManagementInboundPortForPlugin managementReducePluginInboundPort;
	/**uri of inbound port for management*/
	protected String ManagementInPortUri; 
	/**The number of threads in the thread pool used to perform the calculation  */
	protected int nbThread;
	/**Function used to do Reduce calculate*/
	protected BiFunction<Tuple,Tuple, Tuple> fonction_reduce;
	/**A buffer who stores the tuples to be calculated */
	protected BlockingQueue<OrderedTuple>  bufferReceive;
	/**Thread pools used for the execution of function fonction_reduce*/
	protected int indexCalculExector;
	/**outbound port to send tuple of result to Gestion component*/
	protected SendTupleOutboundPort sendTupleobp;
	/**Inbound port to receive tuple from Map component  */
	protected ReceiveTupleWithPluginInboundPort ReceiveTupleInboundPort;
	/**uri of inbound port to receive tuple */
	protected String receiveTupleInPortUri;
	/** The uri of inboundPort of the sendTuple destination,used to do the connection*/
	protected String sendReduceTupleInboundPortUri;

	

	public PluginReduce(String uri,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String inboundPortReceiveTupleuri,String inboundPortSendTupleUri) {

		super();
		this.ManagementInPortUri=uri;
		this.receiveTupleInPortUri=inboundPortReceiveTupleuri;
		this.sendReduceTupleInboundPortUri=inboundPortSendTupleUri;
		this.nbThread=nb;
		this.fonction_reduce=fonction_reduce;
				
	}
	
	
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);

		this.addRequiredInterface(SendTupleServiceCI.class);
		this.sendTupleobp=new SendTupleOutboundPort(this.getOwner());
		this.sendTupleobp.publishPort();
		
		
		
	}
	
	@Override
	public void initialise() throws Exception{
		
		super.initialise();
		this.addOfferedInterface(ManagementCI.class);
		this.managementReducePluginInboundPort = new ManagementInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementReducePluginInboundPort.publishPort();
		
		this.addOfferedInterface(SendTupleServiceCI.class);
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
		this.removeOfferedInterface(ManagementCI.class);
		
		this.ReceiveTupleInboundPort.unpublishPort();
		this.ReceiveTupleInboundPort.destroyPort();
		this.removeOfferedInterface(SendTupleServiceCI.class);

		
		this.sendTupleobp.unpublishPort();
		this.sendTupleobp.destroyPort();
		this.removeRequiredInterface(SendTupleServiceCI.class);
		
	}


	/**
	 * Launch the reduce task , run the code differently depending on the nature of fonction_reduce
	 * @param function BiFunction(Tuple,Tuple, Tuple),the function to be executed
	 * @param t Tuple of data size,Contains two int's, 
	 * one storing the minimum value Function(Tuple, Tuple)of the generated Tuple's id and one storing the maximum value
	 * @param nature  Store the nature of the reduce function,It can be  COMMUTATIVE_ASSOCIATIVE,ASSOCIATIVE or ITERATIVE
	 * @throws Exception exception
	 */
	

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {

		int tupleIdMax=(int) t.getIndiceData(0);
		int tupleIdMin=(int) t.getIndiceData(1);
		this.fonction_reduce=function;
	
		
		switch(nature){
		    case COMMUTATIVE_ASSOCIATIVE :
		    {
		    	bufferReceive=new LinkedBlockingQueue<>();
		        
				for(int i=tupleIdMin;i<tupleIdMax-1;i++) {
					OrderedTuple t1=(OrderedTuple) bufferReceive.take();
					OrderedTuple t2=(OrderedTuple) bufferReceive.take();
					
					this.getOwner().runTask(indexCalculExector,reduce -> {try {
						((CreateCalculServiceI)reduce).createReduceCalculTask((LinkedBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
					} catch (Exception e) {
						e.printStackTrace();
					}});
					
				}
				OrderedTuple finalResult = bufferReceive.take();
				System.out.println("Component Reduce finished,final result id is " + (finalResult).getId() );
				if(finalResult.getIndiceData(0) instanceof Integer||finalResult.getIndiceData(0) instanceof Double||
						finalResult.getIndiceData(0) instanceof String) {
					System.out.println("final result value is :  " + finalResult.getIndiceData(0) );
				}
				
				this.sendTupleobp.tupleSender(finalResult);
				return true;
		    	
		    }// Fin de case COMMUTATIVE_ASSOCIATIVE
		       
		    case ASSOCIATIVE :
		    {
		    	this.bufferReceive=new PriorityBlockingQueue<OrderedTuple>();
				OrderedTuple tmp1=null;
				OrderedTuple tmp2=null;
				
				for(int i=tupleIdMin;i<tupleIdMax-1;i++) {
					//Store whether two tuples have been found that can be computed
						boolean LastCoupleMatched=false;
						while (!LastCoupleMatched) {
								 tmp1=(OrderedTuple) bufferReceive.take();
								 tmp2=(OrderedTuple) bufferReceive.take();
						/*
						 * Only two Tuples with an id or minRange adjacent to each other can be calculated
						 * */
							if((tmp1.getId()+1)==tmp2.getId()||(tmp1.getId()+1)==tmp2.getRangeMin()||
									(tmp2.getId()+1)==tmp1.getId()||(tmp2.getId()+1)==tmp1.getRangeMin()) {
									LastCoupleMatched=true;
								
							}else {
								bufferReceive.put(tmp1);
								bufferReceive.put(tmp2);
								
							}			
						}
					
						OrderedTuple t1=tmp1;
						OrderedTuple t2=tmp2;
						
						if(t1.getId()<t2.getId()) {
							this.getOwner().runTask(indexCalculExector,reduce -> {try {
								((CreateCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
							} catch (Exception e) {
								e.printStackTrace();
							}});
							
						}else {
		
							this.getOwner().runTask(indexCalculExector,reduce -> {try {
								((CreateCalculServiceI)reduce).createReduceCalculTask((PriorityBlockingQueue<OrderedTuple>) bufferReceive, function, t2, t1);
							} catch (Exception e) {
								e.printStackTrace();
							}});
						}	
						
				}
						
				OrderedTuple finalResult = bufferReceive.take();
				System.out.println("Component Reduce finished,final result id is " + (finalResult).getId());
				this.sendTupleobp.tupleSender(finalResult);
				return true;
		    }  // Fin de case ASSOCIATIVE
		    
		    
		    case ITERATIVE :{
		        bufferReceive=new PriorityBlockingQueue<>();
		        //Record the id of the current tuple to be calculated
				int currentTupleId=tupleIdMin;	     
				        
				for(int i=tupleIdMin;i<tupleIdMax-1;i++) {
					OrderedTuple tmp1=(OrderedTuple) bufferReceive.take();
					while(currentTupleId!=tmp1.getId()) {
						
						bufferReceive.put(tmp1);
						tmp1=(OrderedTuple) bufferReceive.take();
					}
					currentTupleId++;
					OrderedTuple t1=tmp1;
				
					OrderedTuple tmp2=(OrderedTuple) bufferReceive.take();
					while(currentTupleId!=tmp2.getId()) {
						bufferReceive.put(tmp2);
						tmp2=(OrderedTuple) bufferReceive.take();
						
					}
					OrderedTuple t2=tmp2;
					this.getOwner().runTask(indexCalculExector,reduce -> {
						try {
						((CreateCalculServiceI)reduce).createReduceCalculTask((BlockingQueue<OrderedTuple>) bufferReceive, function, t1, t2);
						
					} 
						catch (Exception e) {
						e.printStackTrace();
					}});
					
				}
				OrderedTuple finalResult = bufferReceive.take();
				System.out.println("Component Reduce finished,final result id is" + (finalResult).getId() );
				if(finalResult.getIndiceData(0) instanceof Integer||finalResult.getIndiceData(0) instanceof Double||
						finalResult.getIndiceData(0) instanceof String) {
					System.out.println("final result value is :  " + finalResult.getIndiceData(0) );
				}
				this.sendTupleobp.tupleSender(finalResult);
				return true;
		    }// Fin de ITERATIVE 
		       
		}// Fin de switch
		
		return false;	
       
	}

	/**
	 * receive tuple from Map component  and put the tuple into bufferReceive
	 * @param t Tuple containing the data to be computed
	 * @throws Exception exception
	 */

	public void tupleSender(Tuple t) throws Exception {
		bufferReceive.put((OrderedTuple) t);
		System.out.println("Component reduce receive  :" +((OrderedTuple) t).getId());
		
	}


	/**
	 * Launch the connection between Map component and Gestion component to send Tuple of result
	 * @throws Exception exception
	 */

	@Override
	public void DoPluginPortConnection() throws Exception {
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}
	
	
	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}


	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
		
	}



	

	
	
}