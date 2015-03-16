package org.elasticsearch.river.tibcoas;

import org.elasticsearch.river.tibcoas.ActiveSpacesClientService;
import org.elasticsearch.river.tibcoas.ActiveSpacesRiverDefinition;
import org.elasticsearch.river.tibcoas.SharedContext;
import org.elasticsearch.river.tibcoas.Status;
import org.elasticsearch.river.tibcoas.StatusChecker;
import org.elasticsearch.river.tibcoas.util.ActiveSpacesRiverHelper;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class ActiveSpacesRiver extends AbstractRiverComponent implements
		River 
{
	private static final ESLogger logger = Loggers.getLogger(ActiveSpacesRiver.class);
	protected final Client esClient;
	protected final ActiveSpacesRiverDefinition definition;
	protected final SharedContext context;
	protected final ActiveSpacesClientService activeSpacesClientService;
	public final static String STATUS_ID = "_riverstatus";
	public final static String TYPE = "activespaces";
	public static final String STATUS_FIELD = "status";
	
	protected volatile Thread startupThread;
	protected volatile Thread indexerThread;
	protected volatile Thread statusThread;
	protected volatile Thread cleanupThread;

	@Inject
	public ActiveSpacesRiver(RiverName riverName, RiverSettings riverSettings, Client esClient) 
	{
		super(riverName, riverSettings);
		if (logger.isTraceEnabled()) {
            logger.trace("Initializing river : [{}]", riverName.getName());
        }
		this.esClient = esClient;
		this.definition = ActiveSpacesRiverDefinition.parseSettings(riverName.getName(), riverSettings);
		this.context = new SharedContext(Status.STOPPED);
		this.activeSpacesClientService = new ActiveSpacesClientService(definition);
		
	}
	
	@Override
	public void start() 
	{

		logger.info("Starting river {}", riverName.getName());
		logger.info("TIBCO Activespaces Options : Metaspace[{}], Discovery URL [{}], "
				+ "Listen URL [{}], Space [{}]", definition.getMetaspaceName(), definition.getDiscoveryURL(), definition.getListenURL(), definition.getSpaceName());
		
		Status status = null;
		try 
		{
			status = ActiveSpacesRiverHelper.getRiverStatus(esClient, riverName.getName());
		} 
		catch (NullPointerException e) 
		{
			logger.info("Could not find status field for river : {}", riverName.getName());
			logger.info("Trying to start river : {}", riverName.getName());
		}
		
		if (status == Status.IMPORT_FAILED || status == Status.INITIAL_IMPORT_FAILED || status == Status.START_FAILED) 
		{
            logger.error("Cannot start river {}. Current status is {}", riverName.getName(), status);
            return;
        }
		else if (status == Status.STOPPED) 
		{
            // Leave the current status of the river alone, but set the context status to 'stopped'.
            // Enabling the river via REST will trigger the actual start.
            context.setStatus(Status.STOPPED);

            logger.info("River {} is currently disabled and will not be started", riverName.getName());
        } 
		else 
		{
            // Mark the current status as "waiting for full start"
            context.setStatus(Status.START_PENDING);
            // Request start of the river in the next iteration of the status thread
            ActiveSpacesRiverHelper.setRiverStatus(esClient, riverName.getName(), Status.RUNNING);

            logger.info("River {} startup pending", riverName.getName());
        }

        statusThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activespaces_river_status:" + definition.getIndexName()).newThread(
                new StatusChecker(this, definition, context));
        statusThread.start();
       
	}
	
	void internalStartRiver()
	{
		if (startupThread != null) 
		{
            // Already processing a request to start up the river, so ignore this call.
            return;
        }
        // Update the status: we're busy starting now.
        context.setStatus(Status.STARTING);
        
        Runnable startupRunnable = new Runnable() 
        {
            @Override
            public void run() 
            {
            	logger.info("Starting river {}", riverName.getName());
            	logger.info("ActiveSpaces Options : Discovery URL [{}], Listen URL [{}], Metaspace Name [{}], "
            			+ "Space Name [{}]", definition.getDiscoveryURL(), definition.getListenURL(), 
            			definition.getMetaspaceName(), definition.getSpaceName());
            	
				try 
				{
						// Create the index if it does not exist
						try 
						{
						    if (!esClient.admin().indices().prepareExists(definition.getIndexName()).get().isExists()) 
						    {
						        esClient.admin().indices().prepareCreate(definition.getIndexName()).get();
						    }
						        
						} 
				    	catch (Exception e) 
				      	{
						    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) 
						    {
						        // that's fine
						    } 
						    else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) 
						    {
						        // ok, not recovered yet..., lets start indexing and hope we
						        // recover by the first bulk
						        // TODO: a smarter logic can be to register for cluster
						        // event
						        // listener here, and only start sampling when the
						        // block is removed...
						    } 
						    else 
						    {
						        logger.error("failed to create index [{}], disabling river...", e, definition.getIndexName());
						        ActiveSpacesRiverHelper.setRiverStatus(esClient, definition.getRiverName(), Status.START_FAILED);
			                    context.setStatus(Status.START_FAILED);
						        return;
						    }
				      	}
					   
						// All good, mark the context as "running" now: this
						// status value is used as termination condition for the threads we're going to start now.
						context.setStatus(Status.RUNNING);
					   
						indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activespaces_river_indexer:" + definition.getIndexName()).newThread(
					       new EventProcesserAndIndexer(esClient, definition, context, activeSpacesClientService));
						indexerThread.start();
						logger.info("Started river {}", riverName.getName());
						
				} 
				catch (Throwable t) 
				{
					logger.error("Failed to start river {}", t, riverName.getName());
					ActiveSpacesRiverHelper.setRiverStatus(esClient, definition.getRiverName(), Status.START_FAILED);
                    context.setStatus(Status.START_FAILED);
				}
				finally
				{
					// Startup is fully done
                    startupThread = null;
				}
            }
        };
        startupThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activespaces_river_startup:" + definition.getRiverName()).newThread(
                startupRunnable);
        startupThread.start();
	}
	
	@Override
	public void close() 
	{
		logger.info("Closing river {}", riverName.getName());

        // Stop the status thread completely, it will be re-started by #start()
        if (statusThread != null) 
        {
            statusThread.interrupt();
            statusThread = null;
        }
        
        // Cleanup the other parts (the status thread is gone, and can't do that for us anymore)
        internalStopRiver();
        /*
        Runnable cleanupRunnable = new Runnable(){

			@Override
			public void run() 
			{
				
		        //Stop event browser and close space, metaspace connections
		        logger.info("Stopping event browser...");
		        logger.info("Closing connections to space [{}]...", definition.getSpaceName());
		        try 
		        {
					Thread.sleep(10000);
				} catch (InterruptedException e) 
		        {
					e.printStackTrace();
				}
		      	activeSpacesClientService.cleanup();
		      	
		      	logger.info("Stopped river {}", riverName.getName());
				
			}};
			cleanupThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "activespaces_river_cleanup:" + definition.getRiverName()).newThread(
					cleanupRunnable);
			cleanupThread.start();
			*/
	}


	public void internalStopRiver() 
	{
		logger.info("Stopping river {}", riverName.getName());
        try 
        {
            if (startupThread != null) 
            {
                startupThread.interrupt();
                startupThread = null;
            }
            
            if (indexerThread != null) 
            {
                indexerThread.interrupt();
                indexerThread = null;
            }
          	
        } 
        catch (Throwable t) 
        {
            logger.error("Failed to stop river {}", t, riverName.getName());
        } 
        finally 
        {
            this.context.setStatus(Status.STOPPED);
        }
		
	}
	

}
