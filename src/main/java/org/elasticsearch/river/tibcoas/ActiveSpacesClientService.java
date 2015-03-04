package org.elasticsearch.river.tibcoas;

import java.util.Collection;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.tibco.as.space.ASCommon;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.MemberDef;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.browser.Browser;
import com.tibco.as.space.browser.BrowserDef;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.EventBrowser;
import com.tibco.as.space.browser.EventBrowserDef;

public class ActiveSpacesClientService
{
	private static final ESLogger logger = Loggers.getLogger(ActiveSpacesClientService.class);
	
	private final ActiveSpacesRiverDefinition definition;
	private final Metaspace metaspace;
	private final Space space;
	
	public ActiveSpacesClientService(ActiveSpacesRiverDefinition definition)
	{
		this.definition = definition;
		this.metaspace = connectMetaspace();
		this.space = getSpace();
	}
	
	public EventBrowser getEventBrowser()
	{
		EventBrowser eventBrowser = null;
		EventBrowserDef eventBrowserDef = null;
		if(definition.isInitialImport())
		{
			eventBrowserDef = EventBrowserDef.create().setTimeScope(EventBrowserDef.TimeScope.ALL)
				.setDistributionScope(EventBrowserDef.DistributionScope.ALL)
				.setTimeout(EventBrowserDef.WAIT_FOREVER)
				.setQueryLimit(-1);
		}
		else
		{
			eventBrowserDef = EventBrowserDef.create().setTimeScope(EventBrowserDef.TimeScope.NEW)
					.setDistributionScope(EventBrowserDef.DistributionScope.ALL)
					.setTimeout(EventBrowserDef.WAIT_FOREVER)
					.setQueryLimit(-1);
		}
		
		try 
		{
			eventBrowser = space.browseEvents(eventBrowserDef);
		} 
		catch (ASException e) 
		{
			
			e.printStackTrace();
		}		
		return eventBrowser;
		
	}
	
	public Browser getBrowser()
	{
		//Create AS Browser
       	Browser browser = null;
       	       	
		BrowserDef browserDef = BrowserDef.create().setTimeScope(BrowserDef.TimeScope.SNAPSHOT)
				.setDistributionScope(BrowserDef.DistributionScope.ALL);
		
		try 
		{
			browser = space.browse(BrowserType.GET, browserDef);
		} 
		catch (ASException e) 
		{
			
			e.printStackTrace();
		}
		
		return browser;
		
	}
	
	public Collection<String> getKeyFieldNames()
	{
		SpaceDef spaceDef = null;
		try 
		{
			spaceDef = space.getSpaceDef();
		} 
		catch (ASException e) 
		{
			
			e.printStackTrace();
		}
		
		return spaceDef.getKeyDef().getFieldNames();
	}
	
	
	public Metaspace connectMetaspace()
	{
		Metaspace metaspace = null;
		MemberDef memberDef = MemberDef.create(definition.getMemberName(), definition.getDiscoveryURL(), definition.getListenURL());
		try 
		{
			
			if((metaspace = ASCommon.getMetaspace(definition.getMetaspaceName())) == null)
				metaspace = Metaspace.connect(definition.getMetaspaceName(), memberDef);
			else
				logger.info("++++ Connection to metaspace [{}] already exists! Using same connection...", definition.getMetaspaceName());
		} 
		catch (ASException e) 
		{
			e.printStackTrace();
		}
		return metaspace;
	}
	
	public Space getSpace()
	{
		Space space = null;
		try 
		{
			space = metaspace.getSpace(definition.getSpaceName(), DistributionRole.LEECH);
		} 
		catch (ASException e) 
		{
			e.printStackTrace();
		}
        return space;
	}
	
	
}
