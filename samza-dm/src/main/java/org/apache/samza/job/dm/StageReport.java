package org.apache.samza.job.dm;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class StageReport {

    private String name;
    private String type;
    private String containerid;
    private String host;
    private int runningContainers;
    private long time;

    private int throughput;


    public StageReport(String rawdata) {
        buildReport(rawdata);
    }

    private void buildReport(String data) {
        JsonObject jsondata = new JsonParser().parse(data).getAsJsonObject();
        JsonObject header = jsondata.getAsJsonObject("header");
        this.setType(header.getAsJsonPrimitive("source").getAsString());
        this.setTime(header.getAsJsonPrimitive("time").getAsLong());
        this.setContainerid(header.getAsJsonPrimitive("container-name").getAsString());
        this.setHost(header.getAsJsonPrimitive("host").getAsString());
        this.setName(header.getAsJsonPrimitive("job-name").getAsString());

        if (type.equals("ApplicationMaster")) {
            JsonObject metrics = jsondata.getAsJsonObject("metrics");
            if (metrics.has("org.apache.samza.metrics.ContainerProcessManagerMetrics")){
                JsonObject containerManagerMetrics = metrics.getAsJsonObject("org.apache.samza.metrics.ContainerProcessManagerMetrics");
                this.setRunningContainers(containerManagerMetrics.getAsJsonPrimitive("running-containers").getAsInt());
            }
        } else if (type.contains("TaskName-Partition")) {
            JsonObject metrics = jsondata.getAsJsonObject("metrics");
            JsonObject taskIntanceMetrics = metrics.getAsJsonObject("org.apache.samza.container.TaskInstanceMetrics");
            this.setThroughput(taskIntanceMetrics.getAsJsonPrimitive("messages-actually-processed").getAsInt());
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getContainerid() {
        return containerid;
    }

    public void setContainerid(String containerid) {
        this.containerid = containerid;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getThroughput() {
        return throughput;
    }

    public void setThroughput(int throughput) {
        this.throughput = throughput;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRunningContainers() {
        return runningContainers;
    }

    public void setRunningContainers(int runningContainers) {
        this.runningContainers = runningContainers;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
