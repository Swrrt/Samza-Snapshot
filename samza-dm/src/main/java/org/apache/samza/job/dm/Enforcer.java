package org.apache.samza.job.dm;

public interface Enforcer {
    /**
     * submit application to the cluster
     * @return
     */
    Enforcer submit();

    /**
     * update schema
     */
    void updateSchema(Allocation allocation);
}
