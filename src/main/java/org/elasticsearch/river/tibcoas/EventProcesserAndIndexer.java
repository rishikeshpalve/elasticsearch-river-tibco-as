package org.elasticsearch.river.tibcoas;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import com.tibco.as.space.ASException;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.EventBrowser;
import com.tibco.as.space.event.ExpireEvent;
import com.tibco.as.space.event.PutEvent;
import com.tibco.as.space.event.SpaceEvent;
import com.tibco.as.space.event.TakeEvent;

public class EventProcesserAndIndexer implements Runnable
{
	private final Client esClient;
	private final ActiveSpacesRiverDefinition definition;
	private final ActiveSpacesClientService activeSpacesClientService;
	private final SharedContext context;
	private EventBrowser eventBrowser;
	private Collection<String> keyFieldNames;

	public EventProcesserAndIndexer(Client esClient,
			ActiveSpacesRiverDefinition definition, SharedContext context,
			ActiveSpacesClientService activeSpacesClientService) 
	{
		this.esClient = esClient;
		this.definition = definition;
		this.context = context;
		this.activeSpacesClientService = activeSpacesClientService;
		
	}

	@Override
	public void run()
	{
		while (context.getStatus() == Status.RUNNING) 
		{
			SpaceEvent event;
			try 
			{
				keyFieldNames = activeSpacesClientService.getKeyFieldNames();
				eventBrowser = activeSpacesClientService.getEventBrowser();
				
				while ((event = eventBrowser.next()) != null) 
				{
					switch (event.getType())
			        {
			            case PUT:
			            	
			                PutEvent putEvent = (PutEvent) event;
			                onPutEvent(putEvent);
			                break;
			                
			            case TAKE:
			            	
			            	TakeEvent takeEvent = (TakeEvent) event;
			            	onTakeEvent(takeEvent);
			            	break;
			            	
			            case EXPIRE:
			            	
			            	ExpireEvent expireEvent = (ExpireEvent) event;
			            	onExpireEvent(expireEvent);
			            	break;
			            	
			            case SEED:
			            case UNSEED:
			            default:
			            	break;
			        }
					
					
				}
			}
			catch (ASException e) 
			{

				e.printStackTrace();
			}
		}
	}
	
	private void onPutEvent(PutEvent putEvent)
	{
		Tuple tuple = putEvent.getTuple();
		Collection<String> fieldNames = tuple.getFieldNames();
		Map<String, Object> json = new HashMap<String, Object>();
		
		for (String field : fieldNames) 
		{
			//FieldDef.FieldType fieldType = tuple.getFieldType(field);
			json.put(field, tuple.get(field));
		}
		String id = formId(tuple);
		IndexRequest indexRequest = new IndexRequest(definition.getIndexName(), definition.getTypeName(), id)
										.source(json);
		UpdateRequest updateRequest = new UpdateRequest(definition.getIndexName(), definition.getTypeName(), id)
										.doc(json)
										.upsert(indexRequest);
		
		try 
		{
			esClient.update(updateRequest).get();
			
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		} 
		catch (ExecutionException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	private void onTakeEvent(TakeEvent takeEvent)
	{
		Tuple tuple = takeEvent.getTuple();
		String id = formId(tuple);
		
		esClient.prepareDelete(definition.getIndexName(), definition.getTypeName(), id).execute().actionGet();
		
	}
	
	private void onExpireEvent(ExpireEvent expireEvent)
	{
		Tuple tuple = expireEvent.getTuple();
		String id = formId(tuple);
		
		esClient.prepareDelete(definition.getIndexName(), definition.getTypeName(), id).execute().actionGet();
	}
	
	private String formId(Tuple tuple)
	{
		StringBuilder id = new StringBuilder(""); 
		for (String field : keyFieldNames)
		{
			id.append("_");
			id.append(tuple.get(field));
		}
		return id.toString();
	}
}
