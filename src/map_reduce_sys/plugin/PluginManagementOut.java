package map_reduce_sys.plugin;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorGestion;
import map_reduce_sys.gestion.GestionOutboundPort;
import map_reduce_sys.interfaces.ManagementI;

public class PluginManagementOut  extends AbstractPlugin {

	
	private static final long serialVersionUID = 1L;
	protected GestionOutboundPort gestionOp;
	protected String inboundPortUri;
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
	
		this.gestionOp = new GestionOutboundPort(this.getOwner());
		this.gestionOp.publishPort();
		System.out.println(" plugin management out installed");
	}
	
	public void setInboundPortUri(String uri) throws Exception {
		this.inboundPortUri = uri;
	}
	
	@Override
	public void initialise() throws Exception{
		
		super.initialise();
	}
	

	public void doManagementConnection() throws Exception {
		System.out.println("uri inbound port: "+inboundPortUri);
		this.getOwner().doPortConnection(
				this.gestionOp.getPortURI(),
				this.inboundPortUri, 
				ConnectorGestion.class.getCanonicalName());
		
		System.out.println(" Managenment Connected");
	}
	
	@Override
	public void finalise() throws Exception {		
		this.getOwner().doPortDisconnection(gestionOp.getPortURI());
		
	}
	
	@Override
	public void uninstall() throws Exception {
		this.gestionOp.unpublishPort();
		this.gestionOp.destroyPort();
	}
	
	
	public ManagementI getServicePort() {
		return this.gestionOp;
	}
	
	

}
