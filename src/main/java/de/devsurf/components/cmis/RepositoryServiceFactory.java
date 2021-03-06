package de.devsurf.components.cmis;

import java.math.BigInteger;
import java.util.Map;
import java.util.Properties;

import org.apache.chemistry.opencmis.commons.impl.server.AbstractServiceFactory;
import org.apache.chemistry.opencmis.commons.server.CallContext;
import org.apache.chemistry.opencmis.commons.server.CmisService;
import org.apache.chemistry.opencmis.server.support.CmisServiceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saperion.connector.pool.ConnectionPoolUtil;
import com.saperion.connector.pool.exceptions.FactoryException;

/**
 * CMIS Service Factory.
 */
public class RepositoryServiceFactory extends AbstractServiceFactory {

    /** Default maxItems value for getTypeChildren()}. */
    private static final BigInteger DEFAULT_MAX_ITEMS_TYPES = BigInteger.valueOf(50);

    /** Default depth value for getTypeDescendants(). */
    private static final BigInteger DEFAULT_DEPTH_TYPES = BigInteger.valueOf(-1);

    /**
     * Default maxItems value for getChildren() and other methods returning
     * lists of objects.
     */
    private static final BigInteger DEFAULT_MAX_ITEMS_OBJECTS = BigInteger.valueOf(200);

    /** Default depth value for getDescendants(). */
    private static final BigInteger DEFAULT_DEPTH_OBJECTS = BigInteger.valueOf(10);
    
    private static ConnectionPoolUtil POOL = new ConnectionPoolUtil();
    
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryServiceFactory.class);

    @Override
    public void init(Map<String, String> parameters) {
    	LOG.info("RepositoryServiceFactory initialized. "+parameters);
    	Properties configuration = new Properties();
    	configuration.putAll(parameters);
    	POOL.initialize(configuration);
    }

    @Override
    public void destroy() {
    	LOG.info("RepositoryServiceFactory destroyed.");
    	try {
			POOL.shutdown();
		} catch (FactoryException e) {
			// ignore
			e.printStackTrace();
		}
    }

    @Override
    public CmisService getService(CallContext context) {
    	LOG.info("RepositoryServiceFactory getService: "+context);
        RepositoryService service = new RepositoryService(context, POOL);

        CmisServiceWrapper<RepositoryService> wrapperService = 
                new CmisServiceWrapper<RepositoryService>(service,
                DEFAULT_MAX_ITEMS_TYPES, DEFAULT_DEPTH_TYPES, DEFAULT_MAX_ITEMS_OBJECTS, DEFAULT_DEPTH_OBJECTS);

        return wrapperService;
    }    
}
