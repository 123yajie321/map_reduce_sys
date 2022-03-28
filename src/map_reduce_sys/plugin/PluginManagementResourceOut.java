package map_reduce_sys.plugin;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorResourceGestion;
import map_reduce_sys.gestion.GestionResourceOutboundPort;
import map_reduce_sys.interfaces.ManagementI;

public class PluginManagementResourceOut  extends AbstractPlugin {

	
	private static final long serialVersionUID = 1L;
	protected GestionResourceOutboundPort gestionResOp;
	protected String inboundPortUri;
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
		
		this.addRequiredInterface(ManagementI.class);
		this.gestionResOp = new GestionResourceOutboundPort(this.getOwner());
		this.gestionResOp.publishPort();
		System.out.println(" out install");
	}
	
	public void setInboundPortUri(String uri) throws Exception {
		this.inboundPortUri = uri;
	}
	
	@Override
	public void initialise() throws Exception{
		this.getOwner().doPortConnection(
				this.gestionResOp.getPortURI(),
				this.inboundPortUri, 
				ConnectorResourceGestion.class.getCanonicalName());
		
		System.out.println(" COnnected");
		super.initialise();
	}
	

	
	@Override
	public void finalise() throws Exception {		
		this.getOwner().doPortDisconnection(gestionResOp.getPortURI());
		
	}
	
	@Override
	public void uninstall() throws Exception {
		this.gestionResOp.unpublishPort();
		this.gestionResOp.destroyPort();
		this.removeRequiredInterface(ManagementI.class);
	}
	
	
	
	

}
