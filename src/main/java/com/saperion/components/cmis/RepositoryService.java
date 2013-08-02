package com.saperion.components.cmis;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.chemistry.opencmis.commons.data.ExtensionsData;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderList;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;
import org.apache.chemistry.opencmis.commons.enums.CapabilityChanges;
import org.apache.chemistry.opencmis.commons.enums.CapabilityContentStreamUpdates;
import org.apache.chemistry.opencmis.commons.enums.CapabilityJoin;
import org.apache.chemistry.opencmis.commons.enums.CapabilityQuery;
import org.apache.chemistry.opencmis.commons.enums.CapabilityRenditions;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.exceptions.CmisPermissionDeniedException;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.RepositoryCapabilitiesImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.RepositoryInfoImpl;
import org.apache.chemistry.opencmis.commons.impl.server.AbstractCmisService;
import org.apache.chemistry.opencmis.commons.server.CallContext;

import com.saperion.connector.authentication.Credentials;
import com.saperion.connector.authentication.LicenseType;
import com.saperion.connector.authentication.Session;
import com.saperion.connector.authentication.keys.UsernamePasswordKey;
import com.saperion.connector.pool.ConnectionPoolUtil;
import com.saperion.connector.pool.PooledSession;

/**
 * CMIS Service Implementation.
 */
public class RepositoryService extends AbstractCmisService {
	
	public static final String ROOT_ID = "$$$ROOT$$$";

    private CallContext context;
    
    private PooledSession session;
    
    private Credentials credentials;
    
    private ConnectionPoolUtil pool;
    
    public RepositoryService(CallContext context, ConnectionPoolUtil pool) {
		super();
		this.context = context;
		this.pool = pool;
		this.credentials = new UsernamePasswordKey(context.getUsername(), context.getPassword(), LicenseType.INDEX, "");
	}

    /**
     * Gets the call context.
     */
    public CallContext getContext() {
        return context;
    }
    
    public PooledSession getSession() {
		return session;
	}
    
    public void connect() {
        Session session = null;
        try {
			session = pool.borrowObject(credentials);
			if(session == null) {
				throw new CmisPermissionDeniedException("User couldn't be logged in.");
			}
		} catch (Exception e) {
			throw new CmisPermissionDeniedException("User couldn't be logged in.");
		}
    }
    
    @Override
    public void close() {
    	if(session != null) {
    		try {
				pool.returnObject(credentials, session);
			} catch (Exception e) {
				// ignore?
				e.printStackTrace();
			}    		
    	}
    }

    // --- CMIS Operations ---

    @Override
    public List<RepositoryInfo> getRepositoryInfos(ExtensionsData extension) {
    	connect();
        // very basic repository info set up
        RepositoryInfoImpl repositoryInfo = new RepositoryInfoImpl();

        repositoryInfo.setId("examplev7");
        repositoryInfo.setName("examplev7");
        repositoryInfo.setDescription("This is the repository representing the example v7");

        repositoryInfo.setCmisVersionSupported("1.1");

        repositoryInfo.setProductName("SAPERION");
        repositoryInfo.setProductVersion("7.5");
        repositoryInfo.setVendorName("SAPERION AG");

        repositoryInfo.setRootFolder(ROOT_ID);

        repositoryInfo.setThinClientUri("");

        RepositoryCapabilitiesImpl capabilities = new RepositoryCapabilitiesImpl();
        capabilities.setCapabilityAcl(CapabilityAcl.NONE);
        capabilities.setAllVersionsSearchable(false);
        capabilities.setCapabilityJoin(CapabilityJoin.NONE);
        capabilities.setSupportsMultifiling(false);
        capabilities.setSupportsUnfiling(false);
        capabilities.setSupportsVersionSpecificFiling(false);
        capabilities.setIsPwcSearchable(false);
        capabilities.setIsPwcUpdatable(false);
        capabilities.setCapabilityQuery(CapabilityQuery.NONE);
        capabilities.setCapabilityChanges(CapabilityChanges.NONE);
        capabilities.setCapabilityContentStreamUpdates(CapabilityContentStreamUpdates.ANYTIME);
        capabilities.setSupportsGetDescendants(false);
        capabilities.setSupportsGetFolderTree(false);
        capabilities.setCapabilityRendition(CapabilityRenditions.NONE);

        return Collections.singletonList((RepositoryInfo) repositoryInfo);
    }

    @Override
    public TypeDefinitionList getTypeChildren(String repositoryId, String typeId, Boolean includePropertyDefinitions,
            BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
    	connect();
        // TODO implement
        return null;
    }

    @Override
    public TypeDefinition getTypeDefinition(String repositoryId, String typeId, ExtensionsData extension) {
    	connect();
        // TODO implement
        return null;
    }

    @Override
    public ObjectInFolderList getChildren(String repositoryId, String folderId, String filter, String orderBy,
            Boolean includeAllowableActions, IncludeRelationships includeRelationships, String renditionFilter,
            Boolean includePathSegment, BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
    	connect();
        // TODO implement
        return null;
    }

    @Override
    public List<ObjectParentData> getObjectParents(String repositoryId, String objectId, String filter,
            Boolean includeAllowableActions, IncludeRelationships includeRelationships, String renditionFilter,
            Boolean includeRelativePathSegment, ExtensionsData extension) {
    	connect();
        // TODO implement
        return null;
    }

    @Override
    public ObjectData getObject(String repositoryId, String objectId, String filter, Boolean includeAllowableActions,
            IncludeRelationships includeRelationships, String renditionFilter, Boolean includePolicyIds,
            Boolean includeAcl, ExtensionsData extension) {
    	connect();
        // TODO implement
        return null;
    }
}
