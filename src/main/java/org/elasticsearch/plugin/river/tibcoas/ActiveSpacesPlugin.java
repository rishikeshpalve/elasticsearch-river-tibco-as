package org.elasticsearch.plugin.river.tibcoas;

import org.elasticsearch.river.tibcoas.ActiveSpacesRiverModule;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;


public class ActiveSpacesPlugin extends AbstractPlugin {

	@Override
	public String description() 
	{
		return "TIBCO Activespaces River for Elasticsearch";
	}

	@Override
	public String name() 
	{
		return "river-tibco-activespaces";
	}
	
	/**
	 *  Register TIBCO Activespaces River
	 */
	
	public void onModule(RiversModule module)
	{
		module.registerRiver("tibcoas", ActiveSpacesRiverModule.class);
	}

}
