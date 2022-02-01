package map_reduce_sys;

public class TaskAdd implements Task {
	private Resource resource;
	private Funtion map_f;
	private Funtion reduce_g;
	private String nature;
	@Override
	public Resource getResource() {
		// TODO Auto-generated method stub
		return resource;
	}
	@Override
	public Funtion get_map_f() {
		// TODO Auto-generated method stub
		return map_f;
	}
	@Override
	public Funtion get_reduce_g() {
		// TODO Auto-generated method stub
		return reduce_g;
	}
	@Override
	public String getNature() {
		
		return nature;
	}
	
	
	
	
	
	
	
	
	

}
