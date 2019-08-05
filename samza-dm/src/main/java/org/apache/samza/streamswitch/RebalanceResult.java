package org.apache.samza.streamswitch;

import org.apache.samza.model.delayguarantee.MigrationContext;

import java.util.Map;

public class RebalanceResult {
    private final Map<String, String> taskContainer;
    private final RebalanceResultCode code;
    public enum RebalanceResultCode {
        Migrating,
        ScalingOut,
        NeedScalingOut,
        ScalingIn,
        Unnecessary,
        Unable
    }
    private final MigrationContext migrationContext;
    public RebalanceResult(RebalanceResultCode code, Map<String, String> taskContainer, MigrationContext migrationContext){
        this.taskContainer = taskContainer;
        this.code = code;
        this.migrationContext = migrationContext;
    }
    public RebalanceResult(RebalanceResultCode code, Map<String, String> taskContainer){
        this.taskContainer = taskContainer;
        this.code = code;
        this.migrationContext = null;
    }
    public Map<String, String> getTaskContainer(){
        return taskContainer;
    }
    public RebalanceResultCode getCode(){
        return code;
    }

    public MigrationContext getMigrationContext() {
        return migrationContext;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }

        RebalanceResult rhs = (RebalanceResult) obj;
        return code.equals(rhs.code);
    }
}
