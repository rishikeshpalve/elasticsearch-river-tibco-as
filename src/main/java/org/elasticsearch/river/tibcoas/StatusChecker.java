package org.elasticsearch.river.tibcoas;

import org.elasticsearch.river.tibcoas.ActiveSpacesRiver;
import org.elasticsearch.river.tibcoas.ActiveSpacesRiverDefinition;
import org.elasticsearch.river.tibcoas.util.ActiveSpacesRiverHelper;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class StatusChecker implements Runnable 
{
	private static final ESLogger logger = Loggers.getLogger(StatusChecker.class);
	private final ActiveSpacesRiver activeSpacesRiver;
	private final ActiveSpacesRiverDefinition definition;
	private final SharedContext context;
	
	public StatusChecker(ActiveSpacesRiver activeSpacesRiver,
			ActiveSpacesRiverDefinition definition, SharedContext context) 
	{
		this.activeSpacesRiver = activeSpacesRiver;
		this.definition = definition;
		this.context = context;
	}

	@Override
	public void run() 
	{
		 while (true) 
		 {
	            try 
	            {
	                Status status = ActiveSpacesRiverHelper.getRiverStatus(this.activeSpacesRiver.esClient, this.definition.getRiverName());
	                if (status != this.context.getStatus()) 
	                {
	                    if (status == Status.RUNNING && this.context.getStatus() != Status.STARTING) 
	                    {
	                        logger.trace("About to start river: {}", this.definition.getRiverName());
	                        activeSpacesRiver.internalStartRiver();
	                    } 
	                    else if (status == Status.STOPPED) 
	                    {
	                        logger.info("About to stop river: {}", this.definition.getRiverName());
	                        activeSpacesRiver.internalStopRiver();
	                     }
	                }
	                Thread.sleep(1000L);
	            } 
	            catch (InterruptedException e) 
	            {
	                logger.debug("Status thread interrupted", e, (Object) null);
	                Thread.currentThread().interrupt();
	                break;
	            }

	        }

	}

}
