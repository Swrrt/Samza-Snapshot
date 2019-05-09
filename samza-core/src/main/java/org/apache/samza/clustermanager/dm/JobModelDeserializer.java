package org.apache.samza.clustermanager.dm;

import org.apache.samza.SamzaException;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class JobModelDeserializer {
    public static JobModel deserializeJobModel(String string){
        ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
        JobModel jm;
        try {
            jm = mmapper.readValue(string, JobModel.class);
        } catch (IOException e) {
            throw new SamzaException("failed to read JobModel from ZK", e);
        }
        return jm;
    }
}
