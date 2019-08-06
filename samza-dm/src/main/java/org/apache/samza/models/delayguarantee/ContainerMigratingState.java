package org.apache.samza.models.delayguarantee;

public class ContainerMigratingState {
    public enum ContainerMigratingStateCode{
        Starting,
        Running,
        Migrating,
        WaitingForMigrating,
    }
    ContainerMigratingStateCode code;

    public ContainerMigratingState(ContainerMigratingStateCode code){
        this.code = code;
    }
    public ContainerMigratingStateCode getCode(){
        return code;
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

        ContainerMigratingState state = (ContainerMigratingState) obj;
        return code.equals(state.code);
    }
}
