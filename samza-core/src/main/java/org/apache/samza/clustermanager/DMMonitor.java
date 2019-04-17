package org.apache.samza.clustermanager;

public interface DMMonitor {


    /**
     * Register the worker container URL for monitor to listen
     */
    void registerContainer(String containerId);

    /**
     * De-register the worker container URL
     * This is called when schema updates which cause changes in containers or when container completes task
     */
    void deregisterContainer(String containerId);

    /**
     * Send the cluster resource and stage load information to the scheduler listener
     */
    void updateResourceAndLoad();

    /**
     * Get current available cluster resource from the Resource Manager
     */
    void getClusterResource();

    /**
     * Get current workload of all worker containers registered with montor
     */
    void getContainerLoad();

}
