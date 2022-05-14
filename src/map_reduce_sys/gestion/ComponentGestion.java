package map_reduce_sys.gestion;

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
import map_reduce_sys.ReceiveTupleInboundPort;
import map_reduce_sys.connector.ConnectorCreatePlugin;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createPluginI;
import map_reduce_sys.job.Job;
import map_reduce_sys.plugin.PluginManagementOut;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.plugin.PluginResource;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;

@RequiredInterfaces(required ={ManagementI.class,ReflectionCI.class})
@OfferedInterfaces(offered ={SendTupleServiceI.class})

public class ComponentGestion extends AbstractComponent {

	//public static final String GMOP_URI = "gmop-uri";

	static int pluginid=0;
	protected ThreadPoolExecutor runTaskExecutor;
	long startTime ;
	long endTime;
	ReceiveTupleInboundPort receiveResultInboundPort;
	
	protected ComponentGestion() throws Exception {
		super(3, 0);
		int N=3;
		this.receiveResultInboundPort=new ReceiveTupleInboundPort(this);
		this.receiveResultInboundPort.publishPort();
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
		Job jobAssocCommuJob=createJob(Nature.COMMUTATIVE_ASSOCIATIVE, 1, 10000);
		Job jobAssoc=createJob(Nature.ASSOCIATIVE, 1, 50000);
		Job jobIteratif=createJob(Nature.ITERATIVE, 1, 10000);
		
		//vesion avec plugin
		
		//receive the  final result from component reduce 
		startTime=System.currentTimeMillis();
		System.out.println("Begin");
		
		this.traceMessage("startTime");

		
		/*
		 * ArrayList<createPluginOutboundPort>CreatePluginconnectionsOP=
		 * createConnectionForinstallPlugins(DistributedCVM.URI_PORT_CREATEPLUGIN1
		 * ,DistributedCVM.URI_PORT_CREATEPLUGIN2,DistributedCVM.URI_PORT_CREATEPLUGIN3)
		 * ;
		 */
		
		System.out.println("out port before");
		ArrayList<ReflectionOutboundPort>listReflectionOutboundPort=connectWithComponentCalcul(DistributedCVM.URI_PORT_REFLEXION1,DistributedCVM.URI_PORT_REFLEXION2,DistributedCVM.URI_PORT_REFLEXION3);
		System.out.println("out port created");
		ArrayList<String> managementInboundPortUriList=new ArrayList<>();
		for(int i=0;i<3;i++) {
			String portUri=AbstractPort.generatePortURI();
			managementInboundPortUriList.add(portUri);
		}
		ArrayList<PluginManagementOut> pluginsManagementOut=createPluginsManagementOut(managementInboundPortUriList);
		
		
		
		String ResourceSendInboundPort=AbstractPort.generatePortURI();
		String mapSendInboundPort=AbstractPort.generatePortURI();
		//String ResourceSendInboundPort2=AbstractPort.generatePortURI();
		//String mapSendInboundPort2=AbstractPort.generatePortURI();
		
		this.traceMessage("begin create Plugin");
		
		if(jobAssocCommuJob.getDataGenerator() instanceof Serializable) {

			this.traceMessage("OK!!!!");
		}
		
		
		
		PluginResource pluginResource=createPluginResource(managementInboundPortUriList.get(0), 3, jobAssoc.getDataGenerator(), ResourceSendInboundPort, pluginid);
		pluginid++;
		PluginMap pluginMap=createPluginMap(managementInboundPortUriList.get(1), 1, jobAssoc.getFunctionMap(), ResourceSendInboundPort, mapSendInboundPort, pluginid);
		pluginid++;
		
		ArrayList<String>reduceReceiveInPortList=new ArrayList<String>();
		reduceReceiveInPortList.add(mapSendInboundPort);
		//reduceReceiveInPortList.add(mapSendInboundPort2);
		Tuple pluginReduceinfo=createTuplePluginInfo(7, managementInboundPortUriList.get(2), 6,jobAssoc.getFunctionReduce(),reduceReceiveInPortList,receiveResultInboundPort.getPortURI(), pluginid);
		PluginReduce pluginReduce=createPluginReduce(pluginReduceinfo);
		pluginid++;
		
		listReflectionOutboundPort.get(0).installPlugin(pluginResource);
		listReflectionOutboundPort.get(1).installPlugin(pluginMap);
		listReflectionOutboundPort.get(2).installPlugin(pluginReduce);
		/*
		CreatePluginconnectionsOP.get(0).createPluginResource(managementInboundPortUriList.get(0), 2, jobAssocCommuJob.getDataGenerator(), ResourceSendInboundPort, pluginid);
		pluginid++;
		CreatePluginconnectionsOP.get(1).createPluginMap(managementInboundPortUriList.get(1), 2, jobAssocCommuJobCommuJob.getFunctionMap(), ResourceSendInboundPort, mapSendInboundPort, pluginid);
		pluginid++;
	
		ArrayList<String>reduceReceiveInPortList=new ArrayList<String>();
		reduceReceiveInPortList.add(mapSendInboundPort);
		//reduceReceiveInPortList.add(mapSendInboundPort2);
		
		Tuple pluginReduceinfo=createTuplePluginInfo(7, managementInboundPortUriList.get(2), 3,jobAssocCommuJobCommuJob.getFunctionReduce(),reduceReceiveInPortList,receiveResultInboundPort.getPortURI(), pluginid);
		CreatePluginconnectionsOP.get(2).createPluginReduce(pluginReduceinfo);
		pluginid++;
		
		/*
		cpopResource2.createPluginResource(managementResInboundPort2, 2, data_generator, ResourceSendInboundPort2, pluginid);
		pluginid++;
		
		cpopMap2.createPluginMap(managementMapInboundPort2, 2, f_map, ResourceSendInboundPort2, mapSendInboundPort2, pluginid);
		pluginid++;*/
		
		this.traceMessage("debug1");
		for(PluginManagementOut plugin:pluginsManagementOut) {
			
			this.installPlugin(plugin);
		}
		
		
		//Thread.sleep(10);
		
		this.traceMessage("debug2");
		for(PluginManagementOut plugin:pluginsManagementOut) {
			
			plugin.doManagementConnection();
		}
		this.traceMessage("debug3");
		
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
		this.traceMessage("debug4");	
		
		Runnable taskRessource = () -> {
			 try {
				 pluginsManagementOut.get(0).getServicePort().runTaskResource(jobAssoc.getDataGenerator(), (Tuple) jobAssoc.getDataSize().getIndiceData(0));
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMap = () -> {
			 try {
				 pluginsManagementOut.get(1).getServicePort().runTaskMap(jobAssoc.getFunctionMap(), (Tuple) jobAssoc.getDataSize().getIndiceData(0));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		
		Runnable taskReduce = () -> {
			 try {
				 pluginsManagementOut.get(2).getServicePort().runTaskReduce(jobAssoc.getFunctionReduce(), (Tuple) jobAssoc.getDataSize().getIndiceData(0),jobIteratif.getNature());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		for(ReflectionOutboundPort port:listReflectionOutboundPort) {
			port.doDisconnection();
			port.unpublishPort();
			
		}
		
		
	/*
		
		Runnable taskRessource2 = () -> {
			 try {
				pluginResOut2.getServicePort().runTaskResource(data_generator, sizeTuple2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMap2 = () -> {
			 try {
				 pluginMapOut2.getServicePort().runTaskMap(f_map, sizeTuple2);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		*/
		
		runTaskExecutor.submit(taskRessource);
		runTaskExecutor.submit(taskMap);
		runTaskExecutor.submit(taskReduce);
		//runTaskExecutor.submit(taskRessource2);
		//runTaskExecutor.submit(taskMap2);
		
		
		/*
		 pluginResOut.getResourceServicePort().runTaskResource(data_generator, sizeTuple);
		 pluginMapOut.getResMapServicePort().runTaskMap(f_map, sizeTuple);
		 pluginReduceOut.getReduceServicePort().runTaskReduce(g_reduce, sizeTuple);
		 
		 
		 
		 
		 ResourcereflectionOutboundPort.doDisconnection();
		 MapreflectionOutboundPort.doDisconnection();
		 ReducereflectionOutboundPort.doDisconnection();
		 ResourcereflectionOutboundPort2.doDisconnection();
		 MapreflectionOutboundPort2.doDisconnection();
		 
		/*
		Function<Tuple, Tuple> f_map = a -> {
			Double resDouble = (Double)a.getIndiceData(0)*10.0;
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, resDouble);
			return tuple;
		};
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> {
			Double resDouble = (Double)a.getIndiceData(0)+(Double)b.getIndiceData(0);
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, resDouble);
			return tuple;
		};
		
		Function<Void, Tuple> s_generator = (Void v) -> {
			Object[] randomNum=new Object[10];
			for(int i=0;i<randomNum.length;i++){
				randomNum[i]=(int)(Math.random()*20);
			}	
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, randomNum);
			return tuple;
		};
		 
		SimpleJob job = new SimpleJob(f_map,g_reduce,s_generator,Nature.COMMUTATIVE_ASSOCIATIVE);
		setJob(job);
		*/
		
		/*
		 * ArrayList<Tuple>source=new ArrayList<Tuple>();
		 * 
		 * for(int i=0;i<6;i++) { Tuple tuple=new Tuple(1); Double
		 * randomNum=(double)(Math.random()*20); tuple.setIndiceTuple(0, randomNum);
		 * source.add(tuple); } Tuple tuple=new Tuple(1); tuple.setIndiceTuple(0,
		 * tuple); this.gmop.tupleSender(tuple);
		 */
		
		
	}
		
		
	/*
	 * public boolean recieveTuple(Tuple t) throws InterruptedException {
	 * this.bufferResultMap.put(t); return true; }
	 */
	
	@Override
	public synchronized void finalise() throws Exception {		
		
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			this.receiveResultInboundPort.unpublishPort();
			//this.removeOfferedInterface(SendTupleServiceI.class);
			//this.removeRequiredInterface(ReflectionCI.class);
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
		
	}

	public boolean recieve_Tuple(Tuple t) {
		//System.out.println("Task finished, the final result is : "+t.getIndiceData(0));
		long endTime=System.currentTimeMillis();
		System.out.println("Executetime "+(endTime-startTime)+"ms");   
		return true;
	}
	
	
	
	public ReflectionOutboundPort connectWithComponentCalcul(String inboundPortString) throws Exception {
		
		ReflectionOutboundPort reflectionOutboundPort=new ReflectionOutboundPort(this);
		reflectionOutboundPort.publishPort();
		this.doPortConnection(reflectionOutboundPort.getPortURI(),inboundPortString, ReflectionConnector.class.getCanonicalName());
		return reflectionOutboundPort;
		
	}
	
  public ArrayList<ReflectionOutboundPort>  connectWithComponentCalcul(String ...portUris ) throws Exception {
	    ArrayList<ReflectionOutboundPort> portList=new ArrayList<ReflectionOutboundPort>();
		System.out.println("entre");
	    for(String uri:portUris) {
	    	System.out.println("for");
	    	ReflectionOutboundPort port=new ReflectionOutboundPort(this);
	    	System.out.println("out port created");
			port.publishPort();
			System.out.println("out port published");
			System.out.println("before connect");
			doPortConnection(port.getPortURI(),uri, ReflectionConnector.class.getCanonicalName());
			System.out.println("before connect");
			portList.add(port);
		}
		return portList;	
		
	}
	
	
	
	public ArrayList<createPluginOutboundPort> createConnectionForinstallPlugins(String ...portUris) throws Exception{
		ArrayList<createPluginOutboundPort>portList=new ArrayList<>();
		for(String uri:portUris) {
			createPluginOutboundPort port=new createPluginOutboundPort(this);
			port.publishPort();
			doPortConnection(port.getPortURI(),uri, ConnectorCreatePlugin.class.getCanonicalName());
			portList.add(port);
		}
		return portList;	
	}
		
	
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
	
	public PluginManagementOut createPluginManagementOut(String inboundPortUri) throws Exception {
		
		PluginManagementOut pluginOut=new PluginManagementOut();
		pluginOut.setInboundPortUri(inboundPortUri);
		pluginOut.setPluginURI("PluginOut"+pluginid);
		pluginid++;
		return pluginOut;
		
	}
	
	 public Tuple createTuplePluginInfo(int size, Object ...arg) {
		 Tuple tupleinfo=new Tuple(size);
		 int indice=0;
		 for(Object info:arg) {
			 tupleinfo.setIndiceTuple(indice, info);
			 indice++;
		 }
		return tupleinfo;
		 
	 }
	  /*
	   * nbResource: nombre de composant resource pour generer les donnees
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
			//	int number=(int) (Math.random()*50);
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
		//this.installPlugin(pluginResourceIn);
		//System.out.println("Component res plugin installed"); 
		return pluginResourceIn;
		
	}
	
	
	public PluginMap createPluginMap(String managementMapInboundPort,int nb,Function<Tuple, Tuple> fonction_map,String mapReceiveTupleinboundPorturi,String ReduceReceiveTupleinboundPortUri,int pluginId) throws Exception {
		PluginMap pluginMapIn=new PluginMap(managementMapInboundPort, nb,fonction_map,mapReceiveTupleinboundPorturi,ReduceReceiveTupleinboundPortUri);
		pluginMapIn.setPluginURI("pluginMapIn"+pluginId);
		//this.installPlugin(pluginMapIn);
		//System.out.println("Component map plugin installed"); 
		return pluginMapIn;
	}
	
	
	public PluginReduce createPluginReduce(/*String managementReduceInboundPort,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String ReduceReceiveTupleinboundPorturi,String sendResultinboundPortUri,int pluginId*/ Tuple pluginInfo) throws Exception {
		
		PluginReduce pluginReduceIn=new PluginReduce(pluginInfo);
		pluginReduceIn.setPluginURI("pluginReduceIn"+pluginInfo.getIndiceData(pluginInfo.getDimension()-1));
		return pluginReduceIn;
		//this.installPlugin(pluginReduceIn);
		//System.out.println("Component reduce plugin installed"); 
		
	}
	 
	
	
	

}
