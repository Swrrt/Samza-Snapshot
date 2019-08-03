package org.apache.samza.job.dm.StreamSwitch;

import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;

//Output JobModel in format:
//  ContainerId: TaskId, TaskId, ...
//  ContainerId: TaskId, TaskId, ...
//  ...
public class JobModelDemonstrator {
    public static void demoJobModel(JobModel jobModel){
        //Map<String, List<String> > containers = new TreeMap<>();
        System.out.printf("JobModel:\n");
        for(ContainerModel container: jobModel.getContainers().values()){
            System.out.printf("%s:", container.getProcessorId());
            //List<String> list = new LinkedList<>();
            for(TaskModel task: container.getTasks().values()){
                System.out.printf("  %s,",task.getTaskName().getTaskName());
                //list.add(task.getTaskName().getTaskName());
            }
            System.out.printf("\n");
            //containers.put(container.getProcessorId(), task);
        }
    }
}
