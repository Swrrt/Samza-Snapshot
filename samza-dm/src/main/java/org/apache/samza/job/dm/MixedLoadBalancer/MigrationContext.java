package org.apache.samza.job.dm.MixedLoadBalancer;

import java.util.HashMap;
import java.util.Map;

public class MigrationContext {
    private final String srcContainer, tgtContainer;
    private final Map<String, String> migratingTasks;
    private boolean isDeployed;
    public MigrationContext(){
        migratingTasks = new HashMap<>();
        srcContainer = "";
        tgtContainer = "";
        isDeployed = false;
    }
    public MigrationContext(String srcContainer, String tgtContainer, Map<String, String> migratingTasks){
        this.migratingTasks = migratingTasks;
        this.srcContainer = srcContainer;
        this.tgtContainer = tgtContainer;
        isDeployed = false;
    }
    public void setDeployed(){
        isDeployed = true;
    }
    public boolean isDeployed(){
        return isDeployed;
    }
    public String getSrcContainer(){
        return srcContainer;
    }
    public String getTgtContainer(){
        return tgtContainer;
    }
    public Map<String, String> getMigratingTasks(){
        return migratingTasks;
    }
}
