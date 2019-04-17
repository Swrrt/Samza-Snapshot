package org.apache.samza.job.dm;

import org.apache.samza.config.Config;

/**
 * Generate enforcer based on the cluster type
 */

public interface EnforcerFactory {

    /**
     * submit job to the cluster and return the enforcer URL
     */
    Enforcer getEnforcer(Config config);

}
