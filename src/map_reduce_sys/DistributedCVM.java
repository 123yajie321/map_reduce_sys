package map_reduce_sys;
import org.apache.commons.math3.exception.util.DummyLocalizable;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import map_reduce_sys.componant.ComponentCalcul;
import map_reduce_sys.gestion.ComponentGestion;

public class DistributedCVM extends AbstractDistributedCVM{
	public static final String URI_PORT_REFLEXION1="COMPONENT1";
	public static final String URI_PORT_REFLEXION2="COMPONENT2";
	public static final String URI_PORT_REFLEXION3="COMPONENT3";
	
	public static final String GESTION_JVM_URI="gestion";
	public static final String RESOURCE_JVM_URI="resource";
	public static final String MAP_JVM_URI="map";
	public static final String REDUCE_JVM_URI="reduce";
	
	
	public DistributedCVM(String [] args) throws Exception {
		
		super(args);	
	}
	
	@Override
	public void instantiateAndPublish() throws Exception {
		if(AbstractDistributedCVM.getThisJVMURI().equals(GESTION_JVM_URI)) {
			AbstractComponent.createComponent(ComponentGestion.class.getCanonicalName(), new Object[] {});
		}else if (AbstractDistributedCVM.getThisJVMURI().equals(RESOURCE_JVM_URI)) {
			AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION1});
		}else if (AbstractDistributedCVM.getThisJVMURI().equals(MAP_JVM_URI)) {
			AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION2});
		}else if (AbstractDistributedCVM.getThisJVMURI().equals(REDUCE_JVM_URI)) {
			AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION3});
		}else {
			System.out.println("Unknow JVM URI : " + AbstractDistributedCVM.getThisJVMURI());
		}
		super.instantiateAndPublish();
		
	}
	
	@Override
	public void interconnect()throws Exception {
		super.interconnect();
		
	}
	
	
	
	public static void main(String[] args) {
		
		try {
			DistributedCVM c = new DistributedCVM(args);
			c.startStandardLifeCycle(10000L);
			System.exit(0);
		} catch (Exception e) {
		
			e.printStackTrace();
		}

		
	}

}
