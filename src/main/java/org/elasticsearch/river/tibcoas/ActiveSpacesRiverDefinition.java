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
	
	private static final String DEFAULT_TYPE_NAME = "activespaces";
	private static final String DEFAULT_DISCOVERY_URL = null;
	private static final String DEFAULT_LISTEN_URL = null;
	private static final String DEFAULT_METASPACE_NAME = null;
	private static final String DEFAULT_SPACE_NAME = null;
	private static final boolean DEFAULT_INITIAL_IMPORT = true;
	private static final String DEFAULT_EXCLUDE_FIELDS = null;
	
	private static final String INDEX_OBJECT = "index";
	private static final String NAME_FIELD = "name";
	private static final String TYPE_FIELD = "type";
	
	private static final String OPTIONS_OBJECT = "options";
	private static final String SKIP_IMPORT_FIELD = "skip_initial_import";
	private static final String EXCLUDE_FIELDS_FIELD = "exclude_fields";
	
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
	
	public ActiveSpacesRiverDefinition(final Builder builder) 
	{
		this.riverName = builder.riverName;
		this.indexName = builder.indexName;
		this.typeName = builder.typeName;
		this.metaspaceName = builder.metaspaceName;
		this.discoveryURL = builder.discoveryURL;
		this.listenURL = builder.listenURL;
		this.memberName = builder.memberName;
		this.spaceName = builder.spaceName;
		this.setExcludeFields(builder.excludeFields);
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
		
		public Builder riverName(String riverName)
		{
			this.riverName = riverName;
			return this;
		}
		
		public Builder indexName(String indexName)
		{
			this.indexName = indexName;
			return this;
		}
		
		public Builder typeName(String typeName)
		{
			this.typeName = typeName;
			return this;
		}
		
		public Builder metaspaceName(String metaspaceName)
		{
			this.metaspaceName = metaspaceName;
			return this;
		}
		
		public Builder discoveryURL(String discoveryURL)
		{
			this.discoveryURL = discoveryURL;
			return this;
		}
		
		public Builder listenURL(String listenURL)
		{
			this.listenURL = listenURL;
			return this;
		}
		
		public Builder memberName(String memberName)
		{
			this.memberName = memberName;
			return this;
		}
		
		public Builder spaceName(String spaceName)
		{
			this.spaceName = spaceName;
			return this;
		}
		
		public Builder initialImport(boolean initialImport)
		{
			this.initialImport = initialImport;
			return this;
		}
		
		public Builder excludeFields(Set<String> excludeFields) 
		{
			this.excludeFields = excludeFields;
			return this;
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
			builder.indexName = XContentMapValues.nodeStringValue(indexSettings.get(NAME_FIELD), riverName);
			builder.typeName = XContentMapValues.nodeStringValue(indexSettings.get(TYPE_FIELD), DEFAULT_TYPE_NAME);
		}
		else
		{
			builder.indexName = riverName;
			builder.typeName = DEFAULT_TYPE_NAME;
		}
		
		if(riverSettings.settings().containsKey(OPTIONS_OBJECT))
		{
			Map<String, Object> optionsSettings = (Map<String, Object>) riverSettings.settings().get(OPTIONS_OBJECT);
			builder.initialImport = !(XContentMapValues.nodeBooleanValue(optionsSettings.get(SKIP_IMPORT_FIELD), DEFAULT_INITIAL_IMPORT));
			if(optionsSettings.containsKey(EXCLUDE_FIELDS_FIELD)) 
			{
				Set<String> excludeFields = new HashSet<String>();
                Object excludeFieldsSettings = optionsSettings.get(EXCLUDE_FIELDS_FIELD);
                logger.trace("excludeFieldsSettings: " + excludeFieldsSettings);
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
                builder.excludeFields = excludeFields;
			}
		}
		else
		{
			builder.initialImport = DEFAULT_INITIAL_IMPORT;
		}
		
		if(riverSettings.settings().containsKey(AS_OBJECT))
		{
			Map<String, Object> asSettings = (Map<String, Object>) riverSettings.settings().get(AS_OBJECT);
			
			if(asSettings.containsKey(AS_DISCOVERY_FIELD))
			{
				builder.discoveryURL = XContentMapValues.nodeStringValue(asSettings.get(AS_DISCOVERY_FIELD), DEFAULT_DISCOVERY_URL);
				Preconditions.checkNotNull(builder.discoveryURL, "TIBCO Activespaces : Discovery URL is not specified");
			}
			else
			{
				logger.error("TIBCO Activespaces : Discovery URL is not specified");
				builder.discoveryURL = DEFAULT_DISCOVERY_URL;
			}
			
			if(asSettings.containsKey(AS_LISTEN_FIELD))
			{
				builder.listenURL = XContentMapValues.nodeStringValue(asSettings.get(AS_LISTEN_FIELD), DEFAULT_LISTEN_URL);
				Preconditions.checkNotNull(builder.listenURL, "TIBCO Activespaces : Listen URL is not specified");
			}
			else
			{
				logger.error("TIBCO Activespaces : Listen URL is not specified");
				builder.listenURL = DEFAULT_LISTEN_URL;
			}
			
			if(asSettings.containsKey(AS_METASPACE_FIELD))
			{
				builder.metaspaceName = XContentMapValues.nodeStringValue(asSettings.get(AS_METASPACE_FIELD), DEFAULT_METASPACE_NAME);
				Preconditions.checkNotNull(builder.metaspaceName, "TIBCO Activespaces : Metaspace Name is not specified");
			}
			else
			{
				logger.error("TIBCO Activespaces : Metaspace Name is not specified");
				builder.metaspaceName = DEFAULT_METASPACE_NAME;
			}
			
			if(asSettings.containsKey(AS_SPACE_FIELD))
			{
				builder.spaceName = XContentMapValues.nodeStringValue(asSettings.get(AS_SPACE_FIELD), DEFAULT_SPACE_NAME);
				Preconditions.checkNotNull(builder.spaceName, "TIBCO Activespaces : Space Name is not specified");
			}
			else
			{
				logger.error("TIBCO Activespaces : Space Name is not specified");
				builder.spaceName = DEFAULT_SPACE_NAME;
			}
			
			if(asSettings.containsKey(AS_MEMBER_FIELD))
			{
				builder.memberName = XContentMapValues.nodeStringValue(asSettings.get("member"), riverName);
			}
			else
			{
				logger.info("TIBCO Activespaces : Member Name is not specified");
				builder.memberName = riverName;
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

}
