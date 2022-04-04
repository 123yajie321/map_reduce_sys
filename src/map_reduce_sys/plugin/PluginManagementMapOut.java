package map_reduce_sys.plugin;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorMapGestion;
import map_reduce_sys.connector.ConnectorResourceGestion;
import map_reduce_sys.gestion.GestionMapOutboundPort;
import map_reduce_sys.gestion.GestionResourceOutboundPort;
import map_reduce_sys.interfaces.ManagementI;

public class PluginManagementMapOut  extends AbstractPlugin {

	
	private static final long serialVersionUID = 1L;
	protected GestionMapOutboundPort gestionMapOp;
	protected String inboundPortUri;
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
		
		this.addRequiredInterface(ManagementI.class);
		this.gestionMapOp = new GestionMapOutboundPort(this.getOwner());
		this.gestionMapOp.publishPort();
		System.out.println(" out install");
	}
	
	public void setInboundPortUri(String uri) throws Exception {
		this.inboundPortUri = uri;
	}
	
	@Override
	public void initialise() throws Exception{
		System.out.println("uri inbound port: "+inboundPortUri);
		this.getOwner().doPortConnection(
				this.gestionMapOp.getPortURI(),
				this.inboundPortUri, 
				ConnectorMapGestion.class.getCanonicalName());
		
		System.out.println(" COnnected");
		super.initialise();
	}
	

	
	@Override
	public void finalise() throws Exception {		
		this.getOwner().doPortDisconnection(gestionMapOp.getPortURI());
		
	}
	
	@Override
	public void uninstall() throws Exception {
		this.gestionMapOp.unpublishPort();
		this.gestionMapOp.destroyPort();
		this.removeRequiredInterface(ManagementI.class);
	}
	
	
	public ManagementI getResMapServicePort() {
		return this.gestionMapOp;
	}
	
	

}
