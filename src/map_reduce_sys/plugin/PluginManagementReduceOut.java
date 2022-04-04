package map_reduce_sys.plugin;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorReduceGestion;
import map_reduce_sys.connector.ConnectorResourceGestion;
import map_reduce_sys.gestion.GestionReduceOutboundPort;
import map_reduce_sys.gestion.GestionResourceOutboundPort;
import map_reduce_sys.interfaces.ManagementI;

public class PluginManagementReduceOut  extends AbstractPlugin {

	
	private static final long serialVersionUID = 1L;
	protected GestionReduceOutboundPort gestionRedOp;
	protected String inboundPortUri;
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
		
		this.addRequiredInterface(ManagementI.class);
		this. gestionRedOp = new GestionReduceOutboundPort(this.getOwner());
		this. gestionRedOp.publishPort();
		System.out.println(" out install");
	}
	
	public void setInboundPortUri(String uri) throws Exception {
		this.inboundPortUri = uri;
	}
	
	@Override
	public void initialise() throws Exception{
		this.getOwner().doPortConnection(
				this. gestionRedOp.getPortURI(),
				this.inboundPortUri, 
				ConnectorReduceGestion.class.getCanonicalName());
		
		System.out.println(" COnnected");
		super.initialise();
	}
	

	
	@Override
	public void finalise() throws Exception {		
		this.getOwner().doPortDisconnection( gestionRedOp.getPortURI());
		
	}
	
	@Override
	public void uninstall() throws Exception {
		this. gestionRedOp.unpublishPort();
		this. gestionRedOp.destroyPort();
		this.removeRequiredInterface(ManagementI.class);
	}
	
	public ManagementI getReduceServicePort() {
		return this. gestionRedOp;
	}
	
	

}
