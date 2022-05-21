package map_reduce_sys.plugin;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorGestion;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.ports.GestionOutboundPort;

/**
 * The class <code>PluginManagementOut</code> implements the client side  plug-in
 * for the <code>ManagementI</code> component interface 
 * @author Yajie LIU, Zimeng ZHANG
 */

public class PluginManagementOut  extends AbstractPlugin {

	
	private static final long serialVersionUID = 1L;
	protected GestionOutboundPort gestionOp;
	protected String inboundPortUri;
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
		this.gestionOp = new GestionOutboundPort(this.getOwner());
		this.gestionOp.publishPort();
	}
	
	/**
	 * Set the uri of the inboundport to which the GestionOutboundPort will connect
	 * @param uri String 
	 * @throws Exception exception
	 */
	public void setInboundPortUri(String uri) throws Exception {
		this.inboundPortUri = uri;
	}
	
	@Override
	public void initialise() throws Exception{
		
		super.initialise();
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
	/**
	 * Launch the connection between Gestion component and calculate component to run Task calculate
	 * @throws Exception exception
	 */
	public void doManagementConnection() throws Exception {
		this.getOwner().doPortConnection(
				this.gestionOp.getPortURI(),
				this.inboundPortUri, 
				ConnectorGestion.class.getCanonicalName());
	}
	

}
