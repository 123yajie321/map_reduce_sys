package map_reduce_sys.componant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;

import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.reflection.connectors.ReflectionConnector;
import fr.sorbonne_u.components.reflection.interfaces.ReflectionCI;
import fr.sorbonne_u.components.reflection.ports.ReflectionOutboundPort;
import map_reduce_sys.DistributedCVM;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementCI;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.plugin.PluginManagementOut;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.plugin.PluginResource;
import map_reduce_sys.ports.ReceiveTupleInboundPort;
import map_reduce_sys.structure.Job;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Task;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>ComponentGestion</code> implements a component that can
 * assign tasks to calculate components and receive tuple of result from Reduce component
 *    
 * @author Yajie LIU, Zimeng ZHANG
 */
@RequiredInterfaces(required ={ManagementCI.class,ReflectionCI.class})
@OfferedInterfaces(offered ={SendTupleServiceCI.class})

public class ComponentGestion extends AbstractComponent {
	/**Generate the plugin id, which is used when setting the plugin URI*/
	private static int pluginid=0;
	/**Thread pools used for launching task of calculation*/
	protected ThreadPoolExecutor runTaskExecutor;
	/**Record the start time for the experimental section*/
	protected long startTime ;
	/**Record the end time for the experimental section*/
	protected long endTime;
	/**Inbound port to receive tuple of result from Reduce component  */
	protected ReceiveTupleInboundPort receiveResultInboundPort;
	
	protected ComponentGestion() throws Exception {
		super(3, 0);
		this.receiveResultInboundPort=new ReceiveTupleInboundPort(this);
		this.receiveResultInboundPort.publishPort();
		int N=3;
		runTaskExecutor = new ThreadPoolExecutor(N, N, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20));
		runTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		this.getTracer().setTitle("Gestion ");
		this.getTracer().setRelativePosition(2, 0);
		this.toggleTracing();
	}
	
	@Override
	public synchronized void start() throws ComponentStartException {

		super.start();	
	}
	
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		/*Create three map-reduce job of different nature*
		 * The second parameter is the number of Resource components to be used in the data generation
		 * The third parameter is the total size of data
		 */ 
		Job jobAssocCommuJob=createJob(Nature.COMMUTATIVE_ASSOCIATIVE, 1, 10000);
		Job jobAssoc=createJob(Nature.ASSOCIATIVE, 1,10000);
		Job jobIteratif=createJob(Nature.ITERATIVE, 1, 10000);
		
		startTime=System.currentTimeMillis();
		this.traceMessage("startTime:"+startTime+"\n");

		//Connecting to calculate components with ReflectionOutboundPort
		ArrayList<ReflectionOutboundPort>listReflectionOutboundPort=connectWithComponentCalcul(DistributedCVM.URI_PORT_REFLEXION1,DistributedCVM.URI_PORT_REFLEXION2,DistributedCVM.URI_PORT_REFLEXION3);
		
		ArrayList<String> managementInboundPortUriList=new ArrayList<>();
		for(int i=0;i<listReflectionOutboundPort.size();i++) {
			String portUri=AbstractPort.generatePortURI();
			managementInboundPortUriList.add(portUri);
		}
		//Create PluginManagementOut for gestion component
		ArrayList<PluginManagementOut> pluginsManagementOut=createPluginsManagementOut(managementInboundPortUriList);
		
		
		/*Create and install ResourcePlugin,Map plugin,Reduce plugin on calculate component */
		String ResourceSendInboundPort=AbstractPort.generatePortURI();
		String mapSendInboundPort=AbstractPort.generatePortURI();
		PluginResource pluginResource=createPluginResource(managementInboundPortUriList.get(0), 3, jobAssoc.getDataGenerator(), ResourceSendInboundPort, pluginid);
		pluginid++;
		PluginMap pluginMap=createPluginMap(managementInboundPortUriList.get(1), 1, jobAssoc.getFunctionMap(), ResourceSendInboundPort, mapSendInboundPort, pluginid);
		pluginid++;
		PluginReduce pluginReduce=createPluginReduce(managementInboundPortUriList.get(2), 6,jobAssoc.getFunctionReduce(),mapSendInboundPort,receiveResultInboundPort.getPortURI(), pluginid);
		pluginid++;
		listReflectionOutboundPort.get(0).installPlugin(pluginResource);
		listReflectionOutboundPort.get(1).installPlugin(pluginMap);
		listReflectionOutboundPort.get(2).installPlugin(pluginReduce);
	
		
		for(PluginManagementOut plugin:pluginsManagementOut) {
			
			this.installPlugin(plugin);
		}
		
		/*
		 * All the plugins have been created and installed, the next step is to start linking them together.
		 *  It takes a little time for the plugins to finish initialising, 
		 *  so if an exception occurs you can let the thread sleep for 100ms
		 * */
		//Thread.sleep(10);
		
		for(PluginManagementOut plugin:pluginsManagementOut) {
			
			plugin.doManagementConnection();
		}
	
		for(PluginManagementOut plugin:pluginsManagementOut) {
			Runnable taskConnect = () -> {
				 try {
					plugin.getServicePort().DoPluginPortConnection();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
			runTaskExecutor.submit(taskConnect);
			
		}
		//submit the calculate jobs
		submitRunCalculTask(pluginsManagementOut.get(0), jobAssoc, Task.ResourceTask, 0);
		submitRunCalculTask(pluginsManagementOut.get(1), jobAssoc, Task.MapTask, 0);
		submitRunCalculTask(pluginsManagementOut.get(2), jobAssoc, Task.ReduceTask, 0);
		
		for(ReflectionOutboundPort port:listReflectionOutboundPort) {
			port.doDisconnection();
			port.unpublishPort();
			port.destroyPort();	
		}
	
	}
		
		
	
	@Override
	public synchronized void finalise() throws Exception {		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			this.receiveResultInboundPort.unpublishPort();
			
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
		
	}

	/**
	 * receive tuple from Reduce component  and calculate the time of execution 
	 * @param t Tuple containing the result
	 */
	public boolean recieve_Tuple(Tuple t) {
		long endTime=System.currentTimeMillis();
		System.out.println("Executetime "+(endTime-startTime)+"ms"); 
		this.traceMessage("endTime: "+endTime+"\n");
		this.traceMessage("Executetime "+(endTime-startTime)+"ms"+"\n");
		return true;
	}
	
	
	/**
	 * submit the calculate task , run the code differently depending on the type of Task
	 * @param pluginOut PluginManagementOut, plug-in used to launch the task
	 * @param job Job contains the functions used by task
	 * @param typetask  store the type of the task,It can be  ResourceTask,MapTask or ReduceTask
	 * @param numero For a single job there may be several resource components that work together, 
	 * numero specifies which one this is
	 */
	
	
	public void submitRunCalculTask(PluginManagementOut pluginOut,Job job,Task typetask,int numero) {

		switch (typetask) {
		case ResourceTask:{
			
			Runnable taskResource = () -> {
				 try {
					 pluginOut.getServicePort().runTaskResource(job.getDataGenerator(), (Tuple) job.getDataSize().getIndiceData(numero));
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
			runTaskExecutor.submit(taskResource);	
			break;
		}
			
		case MapTask:{
			
			Runnable taskMap = () -> {
				 try {
					 pluginOut.getServicePort().runTaskMap(job.getFunctionMap(), (Tuple) job.getDataSize().getIndiceData(numero));
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
			runTaskExecutor.submit(taskMap);
			break;	
		}
		case ReduceTask:{
			
			Runnable taskReduce = () -> {
				 try {
					 pluginOut.getServicePort().runTaskReduce(job.getFunctionReduce(), (Tuple) job.getDataSize().getIndiceData(numero),job.getNature());
				} catch (Exception e) {
				
					e.printStackTrace();
				}
			};
			runTaskExecutor.submit(taskReduce);
			break;		
		}

		default:
			break;
		}
		
	}
	
	
	/**
	 * For the given uri of ports, create a ReflectionOutboundPort to connect with them
	 * @param portUris String, uri of the reflection inbound port  to be connected
	 * @throws Exception exception
	 * @return ArrayList(ReflectionOutboundPort) rerturn the list of  ReflectionOutboundPort created
	 */
	
  public ArrayList<ReflectionOutboundPort>  connectWithComponentCalcul(String ...portUris ) throws Exception {
	    ArrayList<ReflectionOutboundPort> portList=new ArrayList<ReflectionOutboundPort>();
	    for(String uri:portUris) {
	    	ReflectionOutboundPort port=new ReflectionOutboundPort(this);
			port.publishPort();
			doPortConnection(port.getPortURI(),uri, ReflectionConnector.class.getCanonicalName());
			portList.add(port);
		}
		return portList;	
		
	}
		

	/**
	 * For the given uri of ports, use them to create PluginManagementOut
	 * @param inboundPortUris String, uri of the management inbound port of the PluginManagementOut created
	 * @throws Exception exception
	 * @return ArrayList(PluginManagementOut) rerturn the list of  PluginManagementOut created
	 */
	
	public ArrayList<PluginManagementOut> createPluginsManagementOut(ArrayList<String> inboundPortUris) throws Exception {	
		ArrayList<PluginManagementOut>pluginOutList=new ArrayList<>();
		for(String inboundPortUri:inboundPortUris) {
			PluginManagementOut pluginOut=new PluginManagementOut();
			pluginOut.setInboundPortUri(inboundPortUri);
			pluginOut.setPluginURI("PluginOut"+pluginid);
			pluginid++;
			pluginOutList.add(pluginOut);
		}
			
		return pluginOutList;
	}
	

	/**
	 * depending on the parameters given, create map_reduce  job of different nature and size
	 * @param n Nature, define the nature of the function_reduce of job
	 * @param nbResource  number of resource components to generate the data
	 * @param Sizetotal Total amount of data to be generated
	 * @return Job  rerturn the job created
	 */
	
	public Job createJob(Nature n,int nbResource, int Sizetotal ) {
		
		Job job;
		//La  taille de donnee a generer pour chaque composant
		int size=Sizetotal/nbResource;
		Tuple sizeTuple=new Tuple(nbResource);
		for(int i=0;i<nbResource;i++) {
			Tuple sizeTmp=new Tuple(2);
			// range max of Tuple
			sizeTmp.setIndiceTuple(0,size*(i+1));
			// range min of Tuple
			sizeTmp.setIndiceTuple(1, size*i);
			sizeTuple.setIndiceTuple(i, sizeTmp);
		}

		Function<Integer, Tuple> data_generator = (id)->{
			int number=5;
			Tuple tuple=new OrderedTuple(1,id);
			tuple.setIndiceTuple(0, number);
			return tuple;	
		};
		
	
		Function<Integer, Tuple> matrix_generator = (v)->{
			
			int nb_line=2;
		    int [][]matrix=new int[nb_line][nb_line];
			
		    for(int i=0;i<nb_line;i++) {
		    	 for(int j=0;j<nb_line;j++) {
		    		matrix[i][j]= (int) (Math.random()*10);
		    	 }
		    	
		    }
		    
		    Tuple tuple=new OrderedTuple(1);
			tuple.setIndiceTuple(0, matrix);
			return tuple;	
			
		};
				
		Function<Tuple, Tuple> f_map = a -> { 
			OrderedTuple t=(OrderedTuple)a;
			Integer res =(Integer)a.getIndiceData(0)*10;
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, res);
			
			return tuple;
		};
		
		Function<Tuple, Tuple> f_map_matrix = a -> { 
			OrderedTuple matrixTuple=(OrderedTuple)a;
			int[][] matrixOrginal=(int[][]) matrixTuple.getIndiceData(0);
			int[][] matrixResult= (int[][]) matrixTuple.getIndiceData(0);
			for(int i=0;i<matrixResult.length;i++) {
				for(int j=0;j<matrixResult[0].length;j++) {
					matrixResult[i][j]=(matrixOrginal[i][j])*2;
				}
			}
			
			Tuple tuple = new OrderedTuple(1,matrixTuple.getId());
			tuple.setIndiceTuple(0, matrixResult); 
			return tuple;
		};
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce_matrix = (a,b) -> {
			
			OrderedTuple resultTuple;
			OrderedTuple matrixTuple1=(OrderedTuple)a;
			OrderedTuple matrixTuple2=(OrderedTuple)b;
			int[][] matrix1=(int[][]) a.getIndiceData(0);
			int[][] matrix2=(int[][]) b.getIndiceData(0);
			int[][] matrixResult=new int[matrix1.length][matrix2[0].length];
			for(int i=0;i<matrixResult.length;i++) {
				for(int j=0;j<matrixResult[0].length;j++) {
					int sum=0;
					for(int k=0;k<matrix2.length;k++) {
						sum+=matrix1[i][k]*matrix2[k][j];
					}
					matrixResult[i][j]=sum;
				}
			}
			if(matrixTuple1.getId()<matrixTuple2.getId()) {
						//if the MinRange of tuple1 isn't initialed
					if(matrixTuple1.getRangeMin()<0) {
						resultTuple=new OrderedTuple(1, matrixTuple2.getId(), matrixTuple1.getId());
					}else {
						resultTuple=new OrderedTuple(1, matrixTuple2.getId(), matrixTuple1.getRangeMin());
					}	
			}else {
				
				if(matrixTuple2.getRangeMin()<0) {
					resultTuple=new OrderedTuple(1, matrixTuple1.getId(), matrixTuple2.getId());
				}else {
					resultTuple=new OrderedTuple(1, matrixTuple1.getId(), matrixTuple2.getRangeMin());
				}	
			}
			resultTuple.setIndiceTuple(0, matrixResult);
			System.out.println("tuple1 id:  "+matrixTuple1.getId()+" min range: "+matrixTuple1.getRangeMin()+"\n"+
					"tuple2 id:  "+matrixTuple2.getId()+" min range: "+matrixTuple2.getRangeMin()+"\n"+
					"resultTuple id:  "+resultTuple.getId()+" min range: "+resultTuple.getRangeMin()
			
					);
			
			return resultTuple; 
				  
		};
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> {
			OrderedTuple t=(OrderedTuple)b;

			int resInteger =(int)a.getIndiceData(0)+(int)b.getIndiceData(0); 
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, resInteger); 
			return tuple; 
				  
		};
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce_iteratif = (a,b) -> {
			OrderedTuple t=(OrderedTuple)b;

			int resInteger =(int)a.getIndiceData(0)-(int)b.getIndiceData(0); 
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, resInteger); 
			
			return tuple; 
				  
		};
		
		
		switch (n) {
		case COMMUTATIVE_ASSOCIATIVE:
			
			job=new Job(data_generator,f_map,g_reduce,n, sizeTuple);

			break;
			
		case ASSOCIATIVE:
			job=new Job(matrix_generator,f_map_matrix,g_reduce_matrix,n, sizeTuple);
			break;
	
			
		case ITERATIVE:
			job=new Job(data_generator,f_map,g_reduce_iteratif,n, sizeTuple);
			break;

		default:
			job=new Job(data_generator,f_map,g_reduce,n, sizeTuple);
			break;
		}
		
		
		return job;
	}
	
	
	public PluginResource createPluginResource(String managementResourceInboundPort,int nb,Function<Integer, Tuple> data_generator,String mapReceiveTupleinboundPorturi,int pluginId) throws Exception {
		PluginResource pluginResourceIn=new PluginResource(managementResourceInboundPort, nb,data_generator,mapReceiveTupleinboundPorturi);
		pluginResourceIn.setPluginURI("PluginResourceIn"+pluginId);
		return pluginResourceIn;
		
	}
	
	
	public PluginMap createPluginMap(String managementMapInboundPort,int nb,Function<Tuple, Tuple> fonction_map,String mapReceiveTupleinboundPorturi,String ReduceReceiveTupleinboundPortUri,int pluginId) throws Exception {
		PluginMap pluginMapIn=new PluginMap(managementMapInboundPort, nb,fonction_map,mapReceiveTupleinboundPorturi,ReduceReceiveTupleinboundPortUri);
		pluginMapIn.setPluginURI("pluginMapIn"+pluginId);
		return pluginMapIn;
	}
	
	
	public PluginReduce createPluginReduce(String managementReduceInboundPort,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String ReduceReceiveTupleinboundPorturi,String sendResultinboundPortUri,int pluginId) throws Exception {
		
		PluginReduce pluginReduceIn=new PluginReduce(managementReduceInboundPort,nb,fonction_reduce,ReduceReceiveTupleinboundPorturi,sendResultinboundPortUri);
		pluginReduceIn.setPluginURI("pluginReduceIn"+pluginId);
		return pluginReduceIn;	
	}
	 
	
	
	

}
