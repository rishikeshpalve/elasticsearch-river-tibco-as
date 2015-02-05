package org.elasticsearch.river.tibcoas;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class ActiveSpacesRiverModule extends AbstractModule 
{

	@Override
	protected void configure() 
	{
		bind(River.class).to(ActiveSpacesRiver.class).asEagerSingleton();

	}

}
