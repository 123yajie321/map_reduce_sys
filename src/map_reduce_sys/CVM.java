package map_reduce_sys;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import map_reduce_sys.componant.ComponentCalcul;
import map_reduce_sys.componant.ComponentGestion;


public class CVM extends AbstractCVM {
	
	public static final String URI_PORT_REFLEXION1="COMPONENT1";
	public static final String URI_PORT_REFLEXION2="COMPONENT2";
	public static final String URI_PORT_REFLEXION3="COMPONENT3";
	
	
	public CVM() throws Exception {

	}

	
	@Override
	public void deploy() throws Exception {
		
		
		AbstractComponent.createComponent(ComponentGestion.class.getCanonicalName(), new Object[] {});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION1});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION2});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION3});
		super.deploy();
	
	}


	public static void main(String[] args) {
		
		try {
			CVM c = new CVM();
			c.startStandardLifeCycle(10000L);
			System.exit(0);
		} catch (Exception e) {
		
			e.printStackTrace();
		}

	}


}
