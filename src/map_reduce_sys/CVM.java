package map_reduce_sys;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import map_reduce_sys.componant.ComponentCalcul;
import map_reduce_sys.gestion.ComponentGestion;


public class CVM extends AbstractCVM {
	
	public static final String URI_PORT_REFLEXION1="COMPONENT1";
	public static final String URI_PORT_REFLEXION2="COMPONENT2";
	public static final String URI_PORT_REFLEXION3="COMPONENT3";
	public static final String URI_PORT_REFLEXION4="COMPONENT4";
	public static final String URI_PORT_REFLEXION5="COMPONENT5";
	public static final String URI_PORT_REFLEXION6="COMPONENT6";
	
	/*
	public static final String URI_PORT_CREATEPLUGIN1="CREATEPLUGINPORT1";
	public static final String URI_PORT_CREATEPLUGIN2="CREATEPLUGINPORT2";
	public static final String URI_PORT_CREATEPLUGIN3="CREATEPLUGINPORT3";
	public static final String URI_PORT_CREATEPLUGIN4="CREATEPLUGINPORT4";
	public static final String URI_PORT_CREATEPLUGIN5="CREATEPLUGINPORT5";
	*/
	
	public CVM() throws Exception {

	}

	
	@Override
	public void deploy() throws Exception {
		
		
		AbstractComponent.createComponent(ComponentGestion.class.getCanonicalName(), new Object[] {});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION1});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION2});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION3});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION4});
		AbstractComponent.createComponent(ComponentCalcul.class.getCanonicalName(),new Object[] {URI_PORT_REFLEXION5});
		System.out.println("created");
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
