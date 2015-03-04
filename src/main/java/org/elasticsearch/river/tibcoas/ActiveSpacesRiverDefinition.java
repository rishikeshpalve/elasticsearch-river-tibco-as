package org.elasticsearch.river.tibcoas;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.base.Preconditions;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;

public class ActiveSpacesRiverDefinition 
{
	private static final ESLogger logger = Loggers.getLogger(ActiveSpacesRiverDefinition.class);
	
	private static final String DEFAULT_INDEX_NAME = "activespaces";
	private static final String DEFAULT_TYPE_NAME = "activespaces";
	private static final String DEFAULT_DISCOVERY_URL = null;
	private static final String DEFAULT_LISTEN_URL = null;
	private static final String DEFAULT_METASPACE_NAME = null;
	private static final String DEFAULT_SPACE_NAME = null;
	private static final boolean DEFAULT_INITIAL_IMPORT = true;
	private static final Set<String> DEFAULT_EXCLUDE_FIELDS = null;
	private static final String DEFAULT_FILTER_STRING = "";
	
	private static final String INDEX_OBJECT = "index";
	private static final String NAME_FIELD = "name";
	private static final String TYPE_FIELD = "type";
	
	private static final String OPTIONS_OBJECT = "options";
	private static final String SKIP_IMPORT_FIELD = "skip_initial_import";
	private static final String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	private static final String FILTER_FIELD = "filter";
	
	private static final String AS_OBJECT = "activespaces";
	private static final String AS_DISCOVERY_FIELD = "discovery";
	private static final String AS_LISTEN_FIELD = "listen";
	private static final String AS_METASPACE_FIELD = "metaspace";
	private static final String AS_SPACE_FIELD = "space";
	private static final String AS_MEMBER_FIELD = "member";
	
	private String riverName;
	private String indexName;
	private String typeName;
	private String metaspaceName;
	private String discoveryURL;
	private String listenURL;
	private String memberName;
	private String spaceName;
	private boolean initialImport;
	private Set<String> excludeFields;
	private String filterString;
	
	public ActiveSpacesRiverDefinition(final Builder builder) 
	{
		this.setRiverName(builder.riverName);
		this.setIndexName(builder.indexName);
		this.setTypeName(builder.typeName);
		this.setMetaspaceName(builder.metaspaceName);
		this.setDiscoveryURL(builder.discoveryURL);
		this.setListenURL(builder.listenURL);
		this.setMemberName(builder.memberName);
		this.setSpaceName(builder.spaceName);
		this.setExcludeFields(builder.excludeFields);
		this.setInitialImport(builder.initialImport);
		this.setFilterString(builder.filterString);
	}

	
	public static class Builder
	{
		private String riverName;
		private String indexName;
		private String typeName;
		private String metaspaceName;
		private String discoveryURL;
		private String listenURL;
		private String memberName;
		private String spaceName;
		private boolean initialImport;
		private Set<String> excludeFields;
		private String filterString;
		
		public void setRiverName(String riverName)
		{
			this.riverName = riverName;
		}
		
		public void setIndexName(String indexName)
		{
			this.indexName = indexName;			
		}
		
		public void setTypeName(String typeName)
		{
			this.typeName = typeName;			
		}
		
		public void setMetaspaceName(String metaspaceName)
		{
			this.metaspaceName = metaspaceName;			
		}
		
		public void setDiscoveryURL(String discoveryURL)
		{
			this.discoveryURL = discoveryURL;			
		}
		
		public void setListenURL(String listenURL)
		{
			this.listenURL = listenURL;			
		}
		
		public void setMemberName(String memberName)
		{
			this.memberName = memberName;		
		}
		
		public void setSpaceName(String spaceName)
		{
			this.spaceName = spaceName;			
		}
		
		public void setInitialImport(boolean initialImport)
		{
			this.initialImport = initialImport;			
		}
		
		public void setExcludeFields(Set<String> excludeFields) 
		{
			this.excludeFields = excludeFields;			
		}
		
		public void setFilterString(String filterString)
		{
			this.filterString = filterString;			
		}

		public ActiveSpacesRiverDefinition build()
		{
			return new ActiveSpacesRiverDefinition(this);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public synchronized static ActiveSpacesRiverDefinition parseSettings(String riverName, RiverSettings riverSettings)
	{
		logger.trace("Parse river settings for {}", riverName);
		Preconditions.checkNotNull(riverName, "No riverName specified");
		Preconditions.checkNotNull(riverSettings, "No River settings specified");
		
		Builder builder = new Builder();
		
		if(riverSettings.settings().containsKey(INDEX_OBJECT))
		{
			Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get(INDEX_OBJECT);
			builder.setIndexName(XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), DEFAULT_INDEX_NAME));
			builder.setTypeName(XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), DEFAULT_TYPE_NAME));
		}
		else
		{
			builder.setIndexName(DEFAULT_INDEX_NAME);
			builder.setTypeName(DEFAULT_TYPE_NAME);
		}
		
		if(riverSettings.settings().containsKey(OPTIONS_OBJECT))
		{
			Map<String, Object> optionsSettings = (Map<String, Object>) riverSettings.settings().get(OPTIONS_OBJECT);
			builder.setInitialImport(!(XContentMapValues.nodeBooleanValue(optionsSettings.get(SKIP_IMPORT_FIELD), false)));
			builder.setFilterString(XContentMapValues.nodeStringValue(optionsSettings.get(FILTER_FIELD), DEFAULT_FILTER_STRING));
			if(optionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) 
			{
				Set<String> excludeFields = new HashSet<String>();
                Object excludeFieldsSettings = optionsSettings.get(EXCLUDE_FIELDS_FIELD);
                logger.info("excludeFieldsSettings: " + excludeFieldsSettings);
                boolean array = XContentMapValues.isArray(excludeFieldsSettings);

                if (array) 
                {
                    ArrayList<String> fields = (ArrayList<String>) excludeFieldsSettings;
                    for (String field : fields) 
                    {
                        logger.trace("Field: " + field);
                        excludeFields.add(field);
                    }
                }
                builder.setExcludeFields(excludeFields);
			}
			else
			{
				builder.setExcludeFields(DEFAULT_EXCLUDE_FIELDS);
			}
		}
		else
		{
			builder.setInitialImport(DEFAULT_INITIAL_IMPORT);
			builder.setFilterString(DEFAULT_FILTER_STRING);
		}
		
		if(riverSettings.settings().containsKey(AS_OBJECT))
		{
			Map<String, Object> asSettings = (Map<String, Object>) riverSettings.settings().get(AS_OBJECT);
			
			if(asSettings.containsKey(AS_DISCOVERY_FIELD))
			{
				builder.setDiscoveryURL(XContentMapValues.nodeStringValue(asSettings.get(AS_DISCOVERY_FIELD), DEFAULT_DISCOVERY_URL));
				Preconditions.checkNotNull(builder.discoveryURL, "TIBCO Activespaces : Discovery URL is null");
			}
			else
			{
				logger.error("TIBCO Activespaces : Discovery URL is not specified");
				builder.setDiscoveryURL(DEFAULT_DISCOVERY_URL);
			}
			
			if(asSettings.containsKey(AS_LISTEN_FIELD))
			{
				builder.setListenURL(XContentMapValues.nodeStringValue(asSettings.get(AS_LISTEN_FIELD), DEFAULT_LISTEN_URL));
				Preconditions.checkNotNull(builder.listenURL, "TIBCO Activespaces : Listen URL is null");
			}
			else
			{
				logger.error("TIBCO Activespaces : Listen URL is not specified");
				builder.setListenURL(DEFAULT_LISTEN_URL);
			}
			
			if(asSettings.containsKey(AS_METASPACE_FIELD))
			{
				builder.setMetaspaceName(XContentMapValues.nodeStringValue(asSettings.get(AS_METASPACE_FIELD), DEFAULT_METASPACE_NAME));
				Preconditions.checkNotNull(builder.metaspaceName, "TIBCO Activespaces : Metaspace Name is null");
			}
			else
			{
				logger.error("TIBCO Activespaces : Metaspace Name is not specified");
				builder.setMetaspaceName(DEFAULT_METASPACE_NAME);
			}
			
			if(asSettings.containsKey(AS_SPACE_FIELD))
			{
				builder.setSpaceName(XContentMapValues.nodeStringValue(asSettings.get(AS_SPACE_FIELD), DEFAULT_SPACE_NAME));
				Preconditions.checkNotNull(builder.spaceName, "TIBCO Activespaces : Space Name is null");
			}
			else
			{
				logger.error("TIBCO Activespaces : Space Name is not specified");
				builder.setSpaceName(DEFAULT_SPACE_NAME);
			}
			
			if(asSettings.containsKey(AS_MEMBER_FIELD))
			{
				builder.setMemberName(XContentMapValues.nodeStringValue(asSettings.get("member"), riverName));
			}
			else
			{
				logger.info("TIBCO Activespaces : Member Name is not specified. Using river name : {}", riverName);
				builder.setMemberName(riverName);
			}
		}
		
		return builder.build();
	}

	public String getRiverName() {
		return riverName;
	}

	public void setRiverName(String riverName) {
		this.riverName = riverName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getMemberName() {
		return memberName;
	}

	public void setMemberName(String memberName) {
		this.memberName = memberName;
	}

	public String getMetaspaceName() {
		return metaspaceName;
	}

	public void setMetaspaceName(String metaspaceName) {
		this.metaspaceName = metaspaceName;
	}

	public String getDiscoveryURL() {
		return discoveryURL;
	}

	public void setDiscoveryURL(String discoveryURL) {
		this.discoveryURL = discoveryURL;
	}

	public String getListenURL() {
		return listenURL;
	}

	public void setListenURL(String listenURL) {
		this.listenURL = listenURL;
	}

	public String getSpaceName() {
		return spaceName;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public boolean isInitialImport() {
		return initialImport;
	}

	public void setInitialImport(boolean initialImport) {
		this.initialImport = initialImport;
	}

	public Set<String> getExcludeFields() {
		return excludeFields;
	}

	public void setExcludeFields(Set<String> excludeFields) {
		this.excludeFields = excludeFields;
	}

	public String getFilterString() {
		return filterString;
	}

	public void setFilterString(String filterString) {
		this.filterString = filterString;
	}

}
