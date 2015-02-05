package org.elasticsearch.river.tibcoas;


public class SharedContext 
{
	private Status status;

	public SharedContext(Status status) 
	{
		this.status = status;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
}
