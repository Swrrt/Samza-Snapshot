package org.apache.samza.job.dm.StreamSwitch;


import org.jolokia.client.J4pClient;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pReadResponse;

public class JMXclient {
    public static  void main(String[] args)throws Exception{
        String url = args[0];
        String taskId = "";
        J4pClient j4pClient = new J4pClient(url);
        J4pReadRequest req = new J4pReadRequest("org.apache.samza.container.TaskInstanceMetrics", "");
        J4pReadResponse resp = j4pClient.execute(req);
        System.out.println((String)resp.getValue());
    }
}
