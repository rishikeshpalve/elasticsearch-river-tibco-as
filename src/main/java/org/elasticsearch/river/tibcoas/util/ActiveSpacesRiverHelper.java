package org.elasticsearch.river.tibcoas.util;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.river.tibcoas.ActiveSpacesRiver;
import org.elasticsearch.river.tibcoas.Status;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

public class ActiveSpacesRiverHelper 
{
	private static final ESLogger logger = Loggers.getLogger(ActiveSpacesRiverHelper.class);
	
	 public static Status getRiverStatus(Client client, String riverName) {
	        GetResponse statusResponse = client.prepareGet("_river", riverName, ActiveSpacesRiver.STATUS_ID).get();
	        if (!statusResponse.isExists()) 
	        {
	            return Status.UNKNOWN;
	        } 
	        else 
	        {
	            Object obj = XContentMapValues.extractValue(ActiveSpacesRiver.TYPE + "." + ActiveSpacesRiver.STATUS_FIELD,
	                    statusResponse.getSourceAsMap());
	            
	            return Status.valueOf(obj.toString());
	        }
	    }

	    public static void setRiverStatus(Client client, String riverName, Status status) 
	    {
	        logger.info("setRiverStatus called with {} - {}", riverName, status);
	        XContentBuilder xb;
	        try 
	        {
	            xb = jsonBuilder().startObject().startObject(ActiveSpacesRiver.TYPE).field(ActiveSpacesRiver.STATUS_FIELD, status).endObject()
	                    .endObject();
	            client.prepareIndex("_river", riverName, ActiveSpacesRiver.STATUS_ID).setSource(xb).get();
	        } 
	        catch (IOException ioEx) 
	        {
	            logger.error("setRiverStatus failed for river {}", ioEx, riverName);
	        }
	    }

}
