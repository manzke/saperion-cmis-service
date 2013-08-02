package com.saperion.components.cmis;

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
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.data.ExtensionsData;
import org.apache.chemistry.opencmis.commons.data.ObjectData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderData;
import org.apache.chemistry.opencmis.commons.data.ObjectInFolderList;
import org.apache.chemistry.opencmis.commons.data.ObjectParentData;
import org.apache.chemistry.opencmis.commons.data.Properties;
import org.apache.chemistry.opencmis.commons.data.RepositoryInfo;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinition;
import org.apache.chemistry.opencmis.commons.definitions.TypeDefinitionList;
import org.apache.chemistry.opencmis.commons.enums.BaseTypeId;
import org.apache.chemistry.opencmis.commons.enums.CapabilityAcl;
import org.apache.chemistry.opencmis.commons.enums.CapabilityChanges;
import org.apache.chemistry.opencmis.commons.enums.CapabilityContentStreamUpdates;
import org.apache.chemistry.opencmis.commons.enums.CapabilityJoin;
import org.apache.chemistry.opencmis.commons.enums.CapabilityQuery;
import org.apache.chemistry.opencmis.commons.enums.CapabilityRenditions;
import org.apache.chemistry.opencmis.commons.enums.IncludeRelationships;
import org.apache.chemistry.opencmis.commons.exceptions.CmisBaseException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisConstraintException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisPermissionDeniedException;
import org.apache.chemistry.opencmis.commons.exceptions.CmisRuntimeException;
import org.apache.chemistry.opencmis.commons.impl.MimeTypes;
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.saperion.components.cmis.helper.TypeManager;
import com.saperion.connector.SaClassicConnector;
import com.saperion.connector.authentication.Credentials;
import com.saperion.connector.authentication.LicenseType;
import com.saperion.connector.authentication.Session;
import com.saperion.connector.authentication.keys.UsernamePasswordKey;
import com.saperion.connector.pool.ConnectionPoolUtil;
import com.saperion.connector.pool.PooledSession;
import com.saperion.exception.SaAuthenticationException;
import com.saperion.exception.SaBasicException;
import com.saperion.intf.SaDocInfo;
import com.saperion.intf.SaDocInfo.ElementInfo;
import com.saperion.intf.SaDocumentInfo;
import com.saperion.rmi.SaQueryInfo;

/**
 * CMIS Service Implementation.
 */
public class RepositoryService extends AbstractCmisService {

	public static final String ROOT_ID = "$$$ROOT$$$";

	private CallContext context;

	private PooledSession session;

	private Credentials credentials;

	private ConnectionPoolUtil pool;
	
	private TypeManager types = new TypeManager();

	public RepositoryService(CallContext context, ConnectionPoolUtil pool) {
		super();
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
		Session session = null;
		try {
			session = pool.borrowObject(credentials);
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

		repositoryInfo.setId("examplev7");
		repositoryInfo.setName("examplev7");
		repositoryInfo
				.setDescription("This is the repository representing the example v7");

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
		
		return types.getTypesChildren(context, typeId, includePropertyDefinitions, maxItems, skipCount);
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
		if ((offset != null) || (length != null)) {
			throw new CmisInvalidArgumentException(
					"Offset and Length are not supported!"); // they are but not
																// implemented
																// in the poc ;)
		}

		// check id
		if ((objectId == null)) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}
		// after sanity create connection
		connect();
		SaClassicConnector connector = session.connection();

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
		params.put("repository", repositoryId);
		params.put("objectid", objectId);

		List<SaDocumentInfo> result;
		try {
			result = connector.searchHQL(new SaQueryInfo(
					"from :repository r where r.SYSROWID = :objectid", params));
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

		ElementInfo element = contentInfos.getElementInfo(0);
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
			//todo add info for root folder
		}

		// prepare result
		ObjectInFolderListImpl result = new ObjectInFolderListImpl();
		result.setObjects(new ArrayList<ObjectInFolderData>());
		result.setHasMoreItems(false);

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
		params.put("repository", repositoryId);

		List<SaDocumentInfo> items;
		try {
			items = connector.searchHQL(new SaQueryInfo("from :repository r",
					params)); // add paging
		} catch (SaAuthenticationException e) {
			e.printStackTrace();
			throw new CmisPermissionDeniedException(e.getMessage());
		} catch (SaBasicException e) {
			e.printStackTrace();
			throw new CmisRuntimeException(e.getMessage());
		}

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
			objectInFolder.setObject(compileObjectType(context, connector, documentInfo, this));

			result.getObjects().add(objectInFolder);
		}

		result.setNumItems(BigInteger.valueOf(count));

		return result;
	}

	@Override
	public List<ObjectParentData> getObjectParents(String repositoryId,
			String objectId, String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includeRelativePathSegment, ExtensionsData extension) {
		// check id
		if ((objectId == null)) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}

		// we have no parents in example v7 ;)
		return Collections.emptyList();
	}

	@Override
	public ObjectData getObject(String repositoryId, String objectId,
			String filter, Boolean includeAllowableActions,
			IncludeRelationships includeRelationships, String renditionFilter,
			Boolean includePolicyIds, Boolean includeAcl,
			ExtensionsData extension) {
		// check id
		if ((objectId == null)) {
			throw new CmisInvalidArgumentException("Object Id must be set.");
		}
		// after sanity create connection
		connect();
		SaClassicConnector connector = session.connection();

		HashMap<String, Object> params = Maps.newHashMapWithExpectedSize(2);
		params.put("repository", repositoryId);
		params.put("objectid", objectId);

		List<SaDocumentInfo> result;
		try {
			result = connector.searchHQL(new SaQueryInfo(
					"from :repository r where r.SYSROWID = :objectid", params));
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

		return compileObjectType(context, connector, result.get(0), this);
	}

	/**
	 * Compiles an object type object from a file or folder.
	 */
	private ObjectData compileObjectType(CallContext context,
			SaClassicConnector connector, SaDocumentInfo info,
			ObjectInfoHandler objectInfos) {
		ObjectDataImpl result = new ObjectDataImpl();
		ObjectInfoImpl objectInfo = new ObjectInfoImpl();

		result.setProperties(compileProperties(connector, info, objectInfo));

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
			SaDocumentInfo info, ObjectInfoImpl objectInfo) {
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
			String versionId = info.getValue("XHDOC").getStringValue();
			addPropertyId(result, typeId, filter, PropertyIds.OBJECT_ID, id);
			objectInfo.setId(id);

			// name
			String name = info.getValue("EX7_NAME").getStringValue();// add null
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
				ElementInfo contentInfo = contentInfos.getElementInfo(0);
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

			addPropertyId(result, typeId, filter,
					PropertyIds.CONTENT_STREAM_ID, null);

			return result;
		} catch (Exception e) {
			if (e instanceof CmisBaseException) {
				throw (CmisBaseException) e;
			}
			throw new CmisRuntimeException(e.getMessage(), e);
		}
	}

}
