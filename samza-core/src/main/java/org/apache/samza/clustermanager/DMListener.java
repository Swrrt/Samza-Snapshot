package org.apache.samza.clustermanager;

public interface DMListener {

    /**
     * Register the listener to the DMDispatcher
     * After AM container starts, open port for either RPC or REST communication to receive schema from Dispatcher
     * Send the host address and RPC port to DMDispatcher for its usage
     */
    void registerToDM();

    /**
     * Opens port and listen to the update schema call from dispatcher
     */
    void startListener();

    /**
     * set the yarnapplicationmaster
     */
    void setYarnApplicationMaster(YarnApplicationMaster jc);
}
