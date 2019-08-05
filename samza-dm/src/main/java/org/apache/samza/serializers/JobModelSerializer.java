package org.apache.samza.serializers;

import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;

public class JobModelSerializer {
    public static String jobModelToString(JobModel jobModel){
        try {
            ObjectMapper mmapper = SamzaObjectMapper.getObjectMapper();
            String jobModelStr = mmapper.writerWithDefaultPrettyPrinter().writeValueAsString(jobModel);
            return jobModelStr;
        }catch (Exception e){
            writeLog("Error when serialize job model: " + e);
        }
        return "";
    }
    private static void writeLog(String log){
        System.out.println("JobModelSerializer: " + log);
    }
}
