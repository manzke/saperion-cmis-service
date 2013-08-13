package de.devsurf.components.cmis;

import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyBigInteger;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyBoolean;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyDateTime;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyId;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyIdList;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyInteger;
import static com.saperion.components.cmis.helper.PropertiesFiller.addPropertyString;
import static com.saperion.components.cmis.helper.PropertiesFiller.millisToCalendar;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.Acl;
import org.apache.chemistry.opencmis.commons.data.AllowableActions;
import org.apache.chemistry.opencmis.commons.data.BulkUpdateObjectIdAndChangeToken;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.ExtensionsData;
import org.apache.chemistry.opencmis.commons.data.FailedToDeleteData;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderContainer;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderList;
import org.apache.chemistry.opencmis.commons.data.ObjectList;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.RenditionData;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.AclPropagation;
import org.apache.chemistry.opencmis.commons.enums.Action;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;
import org.apache.chemistry.opencmis.commons.enums.CapabilityChanges;
import org.apache.chemistry.opencmis.commons.enums.CapabilityContentStreamUpdates;
import org.apache.chemistry.opencmis.commons.enums.CapabilityJoin;
import org.apache.chemistry.opencmis.commons.enums.CapabilityQuery;
import org.apache.chemistry.opencmis.commons.enums.CapabilityRenditions;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.enums.RelationshipDirection;
import org.apache.chemistry.opencmis.commons.enums.UnfileObject;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisBaseException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisConstraintException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisNotSupportedException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisPermissionDeniedException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.impl.MimeTypes;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.AllowableActionsImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ObjectDataImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ObjectInFolderDataImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ObjectInFolderListImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertiesImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.RepositoryCapabilitiesImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.RepositoryInfoImpl;
import org.apache.chemistry.opencmis.commons.impl.server.AbstractCmisService;
import org.apache.chemistry.opencmis.commons.impl.server.ObjectInfoImpl;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.commons.server.ObjectInfoHandler;
import org.apache.chemistry.opencmis.commons.spi.Holder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.saperion.components.cmis.helper.TypeManager;
import com.saperion.connector.SaClassicConnector;
import com.saperion.connector.authentication.Credentials;
import com.saperion.connector.authentication.LicenseType;
import com.saperion.connector.authentication.keys.UsernamePasswordKey;
import com.saperion.connector.pool.ConnectionPoolUtil;
import com.saperion.connector.pool.PooledSession;
import com.saperion.exception.SaAuthenticationException;
import com.saperion.exception.SaBasicException;
import com.saperion.exception.SaSystemException;
import com.saperion.intf.SaDocAccessData;
import com.saperion.intf.SaDocInfo;
import com.saperion.intf.SaDocInfo.ElementInfo;
import com.saperion.intf.SaDocumentInfo;
import com.saperion.rmi.SaQueryInfo;

/**
 * CMIS Service Implementation.
 */
public class RepositoryService extends AbstractCmisService {

	private static final String REPOSITORY = "cmis";

	public static final String ROOT_ID = "$$$ROOT$$$";

	private CallContext context;

	private PooledSession session;

	private Credentials credentials;

	private ConnectionPoolUtil pool;

	private TypeManager types = new TypeManager();

	private static final Logger LOG = LoggerFactory
			.getLogger(RepositoryService.class);

	public RepositoryService(CallContext context, ConnectionPoolUtil pool) {
		super();
		LOG.info("RepositoryService created.");
		this.context = context;
		this.pool = pool;
		this.credentials = new UsernamePasswordKey(context.getUsername(),
				context.getPassword(), LicenseType.INDEX, "");
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
		try {
			session = (PooledSession) pool.borrowObject(credentials);
			if (session == null) {
				throw new CmisPermissionDeniedException(
						"User couldn't be logged in.");
			}
		} catch (Exception e) {
			throw new CmisPermissionDeniedException(
					"User couldn't be logged in.");
		}
	}

	@Override
	public void close() {
		LOG.info("close! session? " + (session != null));
		if (session != null) {
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

		repositoryInfo.setId(REPOSITORY);
		repositoryInfo.setName(REPOSITORY);
		repositoryInfo
				.setDescription("This is the repository representing the "
						+ REPOSITORY);

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
		capabilities
				.setCapabilityContentStreamUpdates(CapabilityContentStreamUpdates.ANYTIME);
		capabilities.setSupportsGetDescendants(false);
		capabilities.setSupportsGetFolderTree(false);
		capabilities.setCapabilityRendition(CapabilityRenditions.NONE);

		return Collections.singletonList((RepositoryInfo) repositoryInfo);
	}

	@Override
	public TypeDefinitionList getTypeChildren(String repositoryId,
			String typeId, Boolean includePropertyDefinitions,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		connect();

		return types.getTypesChildren(context, typeId,
				includePropertyDefinitions, maxItems, skipCount);
	}

	@Override
	public TypeDefinition getTypeDefinition(String repositoryId, String typeId,
			ExtensionsData extension) {
		connect();

		return types.getTypeDefinition(context, typeId);
	}

	@Override
	public ContentStream getContentStream(String repositoryId, String objectId,
			String streamId, BigInteger offset, BigInteger length,
			ExtensionsData extension) {
		if ((objectId == null) || objectId.isEmpty()) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}
		if ((offset != null) || (length != null)) {
			throw new CmisInvalidArgumentException(
					"Offset and Length are not supported!"); // they are but not
																// implemented
																// in the poc ;)
		}

		// after sanity create connection
		connect();
		SaClassicConnector connector = session.connection();

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
		params.put("objectid", objectId);

		List<SaDocumentInfo> result;
		try {
			result = connector.searchHQL(new SaQueryInfo("from " + REPOSITORY
					+ " r where r.SYSROWID = :objectid", params));
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}

		if (result.size() != 1) {
			throw new CmisObjectNotFoundException("Object with Id [" + objectId
					+ "] wasn't found.");
		}

		SaDocumentInfo documentInfo = result.get(0);
		String versionId = documentInfo.getValue("XHDOC").getStringValue();
		SaDocInfo contentInfos;
		try {
			contentInfos = connector.getDocumentInfo(versionId, false, true);
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}

		if (contentInfos.getElementCount() == 0) {
			throw new CmisConstraintException("Document has no content!");
		}

		ElementInfo element = contentInfos.getElementInfo(1);
		// compile data
		ContentStreamImpl contentStream = new ContentStreamImpl();
		contentStream.setFileName(element.getName());
		contentStream.setLength(BigInteger.valueOf(element.getSize()));
		contentStream.setMimeType(MimeTypes.getMIMEType(element.getName()));
		try {
			contentStream
					.setStream(connector.readDocument(versionId, false, 1));
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}

		return contentStream;
	}

	@Override
	public ObjectInFolderList getChildren(String repositoryId, String folderId,
			String filter, String orderBy, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, BigInteger maxItems,
			BigInteger skipCount, ExtensionsData extension) {
		if ((folderId == null) || folderId.isEmpty()) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}

		// skip and max
		int skip = (skipCount == null ? 0 : skipCount.intValue());
		if (skip < 0) {
			skip = 0;
		}

		int max = (maxItems == null ? Integer.MAX_VALUE : maxItems.intValue());
		if (max < 0) {
			max = Integer.MAX_VALUE;
		}

		if (!folderId.equalsIgnoreCase(ROOT_ID)) {
			throw new CmisObjectNotFoundException("Folder with id [" + folderId
					+ "] wasn't found.");
		}

		connect();
		SaClassicConnector connector = session.connection();

		// set object info of the the folder
		if (context.isObjectInfoRequired()) {
			// todo add info for root folder
		}

		// prepare result
		ObjectInFolderListImpl result = new ObjectInFolderListImpl();
		result.setObjects(new ArrayList<ObjectInFolderData>());
		result.setHasMoreItems(false);

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(0);

		List<SaDocumentInfo> items;
		try {
			items = connector.searchHQL(new SaQueryInfo("from " + REPOSITORY
					+ " r", params)); // add paging
			
			int count = 0;
			for (SaDocumentInfo documentInfo : items) {
				count++;
				
				if (skip > 0) {
					skip--;
					continue;
				}
				
				if (result.getObjects().size() >= max) {
					result.setHasMoreItems(true);
					continue;
				}
				
				// build and add child object
				ObjectInFolderDataImpl objectInFolder = new ObjectInFolderDataImpl();
				objectInFolder.setObject(compileObjectType(context,
						includeAllowableActions, "", connector, documentInfo, this));
				
				result.getObjects().add(objectInFolder);
			}
			
			result.setNumItems(BigInteger.valueOf(count));
			
			return result;
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}
	}

	@Override
	public List<ObjectParentData> getObjectParents(String repositoryId,
			String objectId, String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includeRelativePathSegment, ExtensionsData extension) {
		// check id
		if ((objectId == null) || objectId.isEmpty()) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}

		// we have no parents in example v7 ;)
		return Collections.emptyList();
	}
	

	@Override
	public ObjectData getObjectByPath(String repositoryId, String path,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
        // check path
        if ((path == null) || (!path.startsWith("/"))) {
            throw new CmisInvalidArgumentException("Invalid folder path!");
        }
        
        if(path.length() == 1) {
        	return getRootFolder(this);
        }

        StringTokenizer tok = new StringTokenizer(path, "/");
        if(!tok.hasMoreTokens()) {
        	throw new CmisInvalidArgumentException("Couldn't find Id in path.");
        }
        
        String id = tok.nextToken();

        return getObject(repositoryId, id, filter, includeAllowableActions, includeRelationships, renditionFilter, includePolicyIds, includeAcl, extension);
    }

	@Override
	public ObjectData getObject(String repositoryId, String objectId,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		// check id
		if ((objectId == null) || objectId.isEmpty()) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}
		if (repositoryId == null || repositoryId.isEmpty()) {
			throw new CmisInvalidArgumentException("Repository Id must be set.");
		}

		if (objectId.equalsIgnoreCase(ROOT_ID)) {
			return getRootFolder(this);
		}

		// after sanity create connection
		connect();
		SaClassicConnector connector = session.connection();

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
		params.put("objectid", objectId);

		List<SaDocumentInfo> result;
		try {
			result = connector.searchHQL(new SaQueryInfo(
					"from cmis r where r.SYSROWID = :objectid", params));
			
			if (result.size() != 1) {
				throw new CmisObjectNotFoundException("Object with Id [" + objectId
						+ "] wasn't found.");
			}
			
			return compileObjectType(context, includeAllowableActions, "", connector,
					result.get(0), this);
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}
	}

	private ObjectData getRootFolder(ObjectInfoHandler objectInfos) {
		ObjectDataImpl result = new ObjectDataImpl();
		ObjectInfoImpl objectInfo = new ObjectInfoImpl();
		
		result.setAllowableActions(compileAllowableActions(true, true, null));

		String typeId = TypeManager.FOLDER_TYPE_ID;
		objectInfo.setBaseType(BaseTypeId.CMIS_FOLDER);
		objectInfo.setTypeId(typeId);
		objectInfo.setContentType(null);
		objectInfo.setFileName(null);
		objectInfo.setHasAcl(true);
		objectInfo.setHasContent(false);
		objectInfo.setVersionSeriesId(null);
		objectInfo.setIsCurrentVersion(true);
		objectInfo.setRelationshipSourceIds(null);
		objectInfo.setRelationshipTargetIds(null);
		objectInfo.setRenditionInfos(null);
		objectInfo.setSupportsDescendants(true);
		objectInfo.setSupportsFolderTree(true);
		objectInfo.setSupportsPolicies(false);
		objectInfo.setSupportsRelationships(false);
		objectInfo.setWorkingCopyId(null);
		objectInfo.setWorkingCopyOriginalId(null);

		PropertiesImpl properties = new PropertiesImpl();
		Set<String> filter = Sets.newHashSetWithExpectedSize(0);

		addPropertyId(properties, typeId, filter, PropertyIds.OBJECT_ID,
				ROOT_ID);
		objectInfo.setId(ROOT_ID);

		addPropertyString(properties, typeId, filter, PropertyIds.NAME, "ROOT");
		objectInfo.setName("ROOT");

		addPropertyString(properties, typeId, filter, PropertyIds.CREATED_BY,
				"system"); // switch to SYSCREATEUSER
		addPropertyString(properties, typeId, filter,
				PropertyIds.LAST_MODIFIED_BY, "system");
		objectInfo.setCreatedBy("system");

		// creation and modification date
		GregorianCalendar lastModified = millisToCalendar(System
				.currentTimeMillis());
		addPropertyDateTime(properties, typeId, filter,
				PropertyIds.CREATION_DATE, lastModified); // SYSCREATEDATE
		addPropertyDateTime(properties, typeId, filter,
				PropertyIds.LAST_MODIFICATION_DATE, lastModified); // SYSTIMESTAMP
		objectInfo.setCreationDate(lastModified);
		objectInfo.setLastModificationDate(lastModified);

		// change token - always null
		addPropertyString(properties, typeId, filter, PropertyIds.CHANGE_TOKEN,
				null);

		// CMIS 1.1 properties
		addPropertyString(properties, typeId, filter, PropertyIds.DESCRIPTION,
				null);
		addPropertyIdList(properties, typeId, filter,
				PropertyIds.SECONDARY_OBJECT_TYPE_IDS, null);

		// base type and type name
		addPropertyId(properties, typeId, filter, PropertyIds.BASE_TYPE_ID,
				BaseTypeId.CMIS_FOLDER.value());
		addPropertyId(properties, typeId, filter, PropertyIds.OBJECT_TYPE_ID,
				TypeManager.FOLDER_TYPE_ID);
		addPropertyString(properties, typeId, filter, PropertyIds.PATH, "/");

		// folder properties
		addPropertyId(properties, typeId, filter, PropertyIds.PARENT_ID, null);
		objectInfo.setHasParent(false);

		addPropertyIdList(properties, typeId, filter,
				PropertyIds.ALLOWED_CHILD_OBJECT_TYPE_IDS, null);

		result.setProperties(properties);

		if (context.isObjectInfoRequired()) {
			objectInfo.setObject(result);
			objectInfos.addObjectInfo(objectInfo);
		}

		return result;
	}

	private AllowableActions compileAllowableActions(boolean isRoot,
			boolean isFolder, SaDocAccessData access) {
		boolean isReadOnly = access != null && !access.isWriteable();

		Set<Action> aas = EnumSet.noneOf(Action.class);

		addAction(aas, Action.CAN_GET_OBJECT_PARENTS, !isRoot);
		addAction(aas, Action.CAN_GET_PROPERTIES, true);
		addAction(aas, Action.CAN_UPDATE_PROPERTIES, !isReadOnly);
		addAction(aas, Action.CAN_MOVE_OBJECT, !isRoot);
		addAction(aas, Action.CAN_DELETE_OBJECT, !isReadOnly && !isRoot);
		addAction(aas, Action.CAN_GET_ACL, true);

		if (isFolder) {
			addAction(aas, Action.CAN_GET_DESCENDANTS, true);
			addAction(aas, Action.CAN_GET_CHILDREN, true);
			addAction(aas, Action.CAN_GET_FOLDER_PARENT, !isRoot);
			addAction(aas, Action.CAN_GET_FOLDER_TREE, true);
			addAction(aas, Action.CAN_CREATE_DOCUMENT, !isReadOnly);
			addAction(aas, Action.CAN_CREATE_FOLDER, !isReadOnly);
			addAction(aas, Action.CAN_DELETE_TREE, !isReadOnly);
		} else {
			addAction(aas, Action.CAN_GET_CONTENT_STREAM, true);
			addAction(aas, Action.CAN_SET_CONTENT_STREAM, !isReadOnly);
			addAction(aas, Action.CAN_DELETE_CONTENT_STREAM, !isReadOnly);
			addAction(aas, Action.CAN_GET_ALL_VERSIONS, true);
		}

		AllowableActionsImpl result = new AllowableActionsImpl();
		result.setAllowableActions(aas);

		return result;
	}

	private static void addAction(Set<Action> aas, Action action,
			boolean condition) {
		if (condition) {
			aas.add(action);
		}
	}

	/**
	 * Compiles an object type object from a file or folder.
	 * @throws SaAuthenticationException 
	 * @throws SaSystemException 
	 */
	private ObjectData compileObjectType(CallContext context,
			boolean includeAllowableActions, String parent, SaClassicConnector connector,
			SaDocumentInfo info, ObjectInfoHandler objectInfos) throws SaSystemException, SaAuthenticationException {
		ObjectDataImpl result = new ObjectDataImpl();
		ObjectInfoImpl objectInfo = new ObjectInfoImpl();

		result.setProperties(compileProperties(connector, info, parent, objectInfo));

		if (includeAllowableActions) {
			result.setAllowableActions(compileAllowableActions(false, false,
					connector.getDocumentAccessData(objectInfo
							.getVersionSeriesId())));
		}

		if (context.isObjectInfoRequired()) {
			objectInfo.setObject(result);
			objectInfos.addObjectInfo(objectInfo);
		}

		return result;
	}

	/**
	 * Gathers all base properties of a file or folder.
	 */
	private Properties compileProperties(SaClassicConnector connector,
			SaDocumentInfo info, String parent, ObjectInfoImpl objectInfo) {
		if (info == null) {
			throw new IllegalArgumentException("Item must not be null!");
		}

		Set<String> filter = Sets.newHashSetWithExpectedSize(0);

		objectInfo.setBaseType(BaseTypeId.CMIS_DOCUMENT);
		objectInfo.setTypeId(BaseTypeId.CMIS_DOCUMENT.value());
		objectInfo.setHasAcl(false);
		objectInfo.setHasContent(true);
		objectInfo.setHasParent(false);
		objectInfo.setVersionSeriesId(null);
		objectInfo.setIsCurrentVersion(true);
		objectInfo.setRelationshipSourceIds(null);
		objectInfo.setRelationshipTargetIds(null);
		objectInfo.setRenditionInfos(null);
		objectInfo.setSupportsDescendants(false);
		objectInfo.setSupportsFolderTree(false);
		objectInfo.setSupportsPolicies(false);
		objectInfo.setSupportsRelationships(false);
		objectInfo.setWorkingCopyId(null);
		objectInfo.setWorkingCopyOriginalId(null);

		// find base type
		String typeId = null;

		// let's do it
		try {
			PropertiesImpl result = new PropertiesImpl();

			// id
			String id = info.getValue("SYSROWID").getStringValue();// add null
																	// check
			addPropertyId(result, typeId, filter, PropertyIds.OBJECT_ID, id);
			objectInfo.setId(id);

			String versionId = info.getValue("XHDOC").getStringValue();
			addPropertyId(result, typeId, filter,
					PropertyIds.VERSION_SERIES_ID, versionId);
			objectInfo.setVersionSeriesId(versionId);

			// name
			String name = info.getValue("NAME").getStringValue();// add null
																	// check,
			addPropertyString(result, typeId, filter, PropertyIds.NAME, name);
			objectInfo.setName(name);

			// created and modified by
			String username = info.getValue("SYSMODIFYUSER").getStringValue(); // add
																				// null
																				// check
			addPropertyString(result, typeId, filter, PropertyIds.CREATED_BY,
					username); // switch to SYSCREATEUSER
			addPropertyString(result, typeId, filter,
					PropertyIds.LAST_MODIFIED_BY, username);
			objectInfo.setCreatedBy(username);

			// creation and modification date
			GregorianCalendar lastModified = millisToCalendar(System
					.currentTimeMillis());
			addPropertyDateTime(result, typeId, filter,
					PropertyIds.CREATION_DATE, lastModified); // SYSCREATEDATE
			addPropertyDateTime(result, typeId, filter,
					PropertyIds.LAST_MODIFICATION_DATE, lastModified); // SYSTIMESTAMP
			objectInfo.setCreationDate(lastModified);
			objectInfo.setLastModificationDate(lastModified);

			// change token - always null
			addPropertyString(result, typeId, filter, PropertyIds.CHANGE_TOKEN,
					null);

			// CMIS 1.1 properties
			addPropertyString(result, typeId, filter, PropertyIds.DESCRIPTION,
					null);
			addPropertyIdList(result, typeId, filter,
					PropertyIds.SECONDARY_OBJECT_TYPE_IDS, null);

			// base type and type name
			addPropertyId(result, typeId, filter, PropertyIds.BASE_TYPE_ID,
					BaseTypeId.CMIS_DOCUMENT.value()); // ?
			addPropertyId(result, typeId, filter, PropertyIds.OBJECT_TYPE_ID,
					BaseTypeId.CMIS_DOCUMENT.value()); // ?

			// file properties
			addPropertyBoolean(result, typeId, filter,
					PropertyIds.IS_IMMUTABLE, false);
			addPropertyBoolean(result, typeId, filter,
					PropertyIds.IS_LATEST_VERSION, true);
			addPropertyBoolean(result, typeId, filter,
					PropertyIds.IS_MAJOR_VERSION, true);
			addPropertyBoolean(result, typeId, filter,
					PropertyIds.IS_LATEST_MAJOR_VERSION, true);
			addPropertyString(result, typeId, filter,
					PropertyIds.VERSION_LABEL, name);
			addPropertyId(result, typeId, filter,
					PropertyIds.VERSION_SERIES_ID, versionId); // ?
			addPropertyBoolean(result, typeId, filter,
					PropertyIds.IS_VERSION_SERIES_CHECKED_OUT, false);
			addPropertyString(result, typeId, filter,
					PropertyIds.VERSION_SERIES_CHECKED_OUT_BY, null);
			addPropertyString(result, typeId, filter,
					PropertyIds.VERSION_SERIES_CHECKED_OUT_ID, null);
			addPropertyString(result, typeId, filter,
					PropertyIds.CHECKIN_COMMENT, "");

			SaDocInfo contentInfos = connector.getDocumentInfo(versionId,
					false, true);
			if (contentInfos.getElementCount() == 0) {
				addPropertyBigInteger(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_LENGTH, null);
				addPropertyString(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_MIME_TYPE, null);
				addPropertyString(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_FILE_NAME, null);
				addPropertyId(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_ID, null);

				objectInfo.setHasContent(false);
				objectInfo.setContentType(null);
				objectInfo.setFileName(null);
			} else {
				ElementInfo contentInfo = contentInfos.getElementInfo(1);
				addPropertyInteger(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_LENGTH,
						contentInfo.getSize());
				String contentFilename = contentInfo.getName();
				String contentMimeType = MimeTypes.getMIMEType(contentFilename);
				addPropertyString(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_MIME_TYPE, contentMimeType);
				addPropertyString(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_FILE_NAME, contentFilename);
				addPropertyId(result, typeId, filter,
						PropertyIds.CONTENT_STREAM_ID, contentInfo.getDocUid());

				objectInfo.setHasContent(true);
				objectInfo.setContentType(contentMimeType);
				objectInfo.setFileName(contentFilename);
			}

			return result;
		} catch (Exception e) {
			if (e instanceof CmisBaseException) {
				throw (CmisBaseException) e;
			}
			throw new CmisRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public TypeDefinition createType(String repositoryId, TypeDefinition type,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("createType is not supported!");
	}

	@Override
	public TypeDefinition updateType(String repositoryId, TypeDefinition type,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("updateType is not supported!");
	}

	@Override
	public void deleteType(String repositoryId, String typeId,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("deleteType is not supported!");
	}

	@Override
	public List<ObjectInFolderContainer> getDescendants(String repositoryId,
			String folderId, BigInteger depth, String filter,
			Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, ExtensionsData extension) {
		throw new CmisNotSupportedException("getDescendants is not supported!");
	}

	@Override
	public List<ObjectInFolderContainer> getFolderTree(String repositoryId,
			String folderId, BigInteger depth, String filter,
			Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePathSegment, ExtensionsData extension) {
		throw new CmisNotSupportedException("getFolderTree is not supported!");
	}

	@Override
	public ObjectData getFolderParent(String repositoryId, String folderId,
			String filter, ExtensionsData extension) {
		throw new CmisNotSupportedException("getFolderParent is not supported!");
	}

	@Override
	public ObjectList getCheckedOutDocs(String repositoryId, String folderId,
			String filter, String orderBy, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"getCheckedOutDocs is not supported!");
	}

	@Override
	public String createDocument(String repositoryId, Properties properties,
			String folderId, ContentStream contentStream,
			VersioningState versioningState, List<String> policies,
			Acl addAces, Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException("createDocument is not supported!");
	}

	@Override
	public String createDocumentFromSource(String repositoryId,
			String sourceId, Properties properties, String folderId,
			VersioningState versioningState, List<String> policies,
			Acl addAces, Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"createDocumentFromSource is not supported!");
	}

	@Override
	public String createFolder(String repositoryId, Properties properties,
			String folderId, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException("createFolder is not supported!");
	}

	@Override
	public String createRelationship(String repositoryId,
			Properties properties, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"createRelationship is not supported!");
	}

	@Override
	public String createPolicy(String repositoryId, Properties properties,
			String folderId, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException("createPolicy is not supported!");
	}

	@Override
	public String createItem(String repositoryId, Properties properties,
			String folderId, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException("createItem is not supported!");
	}

	@Override
	public List<RenditionData> getRenditions(String repositoryId,
			String objectId, String renditionFilter, BigInteger maxItems,
			BigInteger skipCount, ExtensionsData extension) {
		throw new CmisNotSupportedException("getRenditions is not supported!");
	}

	@Override
	public void updateProperties(String repositoryId, Holder<String> objectId,
			Holder<String> changeToken, Properties properties,
			ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"updateProperties is not supported!");
	}

	@Override
	public List<BulkUpdateObjectIdAndChangeToken> bulkUpdateProperties(
			String repositoryId,
			List<BulkUpdateObjectIdAndChangeToken> objectIdAndChangeToken,
			Properties properties, List<String> addSecondaryTypeIds,
			List<String> removeSecondaryTypeIds, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"bulkUpdateProperties is not supported!");
	}

	@Override
	public void moveObject(String repositoryId, Holder<String> objectId,
			String targetFolderId, String sourceFolderId,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("moveObject is not supported!");
	}

	@Override
	public void deleteObjectOrCancelCheckOut(String repositoryId,
			String objectId, Boolean allVersions, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"deleteObjectOrCancelCheckOut is not supported!");
	}

	@Override
	public FailedToDeleteData deleteTree(String repositoryId, String folderId,
			Boolean allVersions, UnfileObject unfileObjects,
			Boolean continueOnFailure, ExtensionsData extension) {
		throw new CmisNotSupportedException("deleteTree is not supported!");
	}

	@Override
	public void setContentStream(String repositoryId, Holder<String> objectId,
			Boolean overwriteFlag, Holder<String> changeToken,
			ContentStream contentStream, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"setContentStream is not supported!");
	}

	@Override
	public void appendContentStream(String repositoryId,
			Holder<String> objectId, Holder<String> changeToken,
			ContentStream contentStream, boolean isLastChunk,
			ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"appendContentStream is not supported!");
	}

	@Override
	public void deleteContentStream(String repositoryId,
			Holder<String> objectId, Holder<String> changeToken,
			ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"deleteContentStream is not supported!");
	}

	@Override
	public void checkOut(String repositoryId, Holder<String> objectId,
			ExtensionsData extension, Holder<Boolean> contentCopied) {
		throw new CmisNotSupportedException("checkOut is not supported!");
	}

	@Override
	public void cancelCheckOut(String repositoryId, String objectId,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("cancelCheckOut is not supported!");
	}

	@Override
	public void checkIn(String repositoryId, Holder<String> objectId,
			Boolean major, Properties properties, ContentStream contentStream,
			String checkinComment, List<String> policies, Acl addAces,
			Acl removeAces, ExtensionsData extension) {
		throw new CmisNotSupportedException("checkIn is not supported!");
	}

	@Override
	public ObjectData getObjectOfLatestVersion(String repositoryId,
			String objectId, String versionSeriesId, Boolean major,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"getObjectOfLatestVersion is not supported!");
	}

	@Override
	public List<ObjectData> getAllVersions(String repositoryId,
			String objectId, String versionSeriesId, String filter,
			Boolean includeAllowableActions, ExtensionsData extension) {
		throw new CmisNotSupportedException("getAllVersions is not supported!");
	}

	@Override
	public ObjectList getContentChanges(String repositoryId,
			Holder<String> changeLogToken, Boolean includeProperties,
			String filter, Boolean includePolicyIds, Boolean includeAcl,
			BigInteger maxItems, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"getContentChanges is not supported!");
	}

	@Override
	public ObjectList query(String repositoryId, String statement,
			Boolean searchAllVersions, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		throw new CmisNotSupportedException("query is not supported!");
	}

	@Override
	public void addObjectToFolder(String repositoryId, String objectId,
			String folderId, Boolean allVersions, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"addObjectToFolder is not supported!");
	}

	@Override
	public void removeObjectFromFolder(String repositoryId, String objectId,
			String folderId, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"removeObjectFromFolder is not supported!");
	}

	@Override
	public ObjectList getObjectRelationships(String repositoryId,
			String objectId, Boolean includeSubRelationshipTypes,
			RelationshipDirection relationshipDirection, String typeId,
			String filter, Boolean includeAllowableActions,
			BigInteger maxItems, BigInteger skipCount, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"getObjectRelationships is not supported!");
	}

	@Override
	public Acl applyAcl(String repositoryId, String objectId, Acl addAces,
			Acl removeAces, AclPropagation aclPropagation,
			ExtensionsData extension) {
		throw new CmisNotSupportedException("applyAcl is not supported!");
	}

	@Override
	public Acl applyAcl(String repositoryId, String objectId, Acl aces,
			AclPropagation aclPropagation) {
		throw new CmisNotSupportedException("applyAcl is not supported!");
	}

	@Override
	public Acl getAcl(String repositoryId, String objectId,
			Boolean onlyBasicPermissions, ExtensionsData extension) {
		throw new CmisNotSupportedException("getAcl is not supported!");
	}

	@Override
	public void applyPolicy(String repositoryId, String policyId,
			String objectId, ExtensionsData extension) {
		throw new CmisNotSupportedException("applyPolicy is not supported!");
	}

	@Override
	public List<ObjectData> getAppliedPolicies(String repositoryId,
			String objectId, String filter, ExtensionsData extension) {
		throw new CmisNotSupportedException(
				"getAppliedPolicies is not supported!");
	}

	@Override
	public void removePolicy(String repositoryId, String policyId,
			String objectId, ExtensionsData extension) {
		throw new CmisNotSupportedException("removePolicy is not supported!");
	}
}
