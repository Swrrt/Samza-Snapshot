package org.apache.samza.streamswitch;

import org.apache.hadoop.conf.Configuration;
import org.apache.samza.config.Config;
import org.apache.samza.job.yarn.YarnJob;

public class ScalableYarnJob extends YarnJob {
    Config config;
    public ScalableYarnJob(Config config, Configuration hadoopConfig){
        super(config, hadoopConfig);
        this.config = config;
    }

    void startStreamSwitchOnNewThread(){
        System.out.println("Starting StreamSwitch one new thread");
        StreamSwitch streamSwitch = new StreamSwitch();
        // Scheduler usually keep running until being killed
        Thread thread = new Thread(){
            public void run(){
                try{
                    System.out.println("Starting StreamSwitch one new thread");
                    streamSwitch.init(config);
                    streamSwitch.start();
                }catch (Exception e){
                    System.out.println(e);
                }
            }
        };
        thread.start();
    }
    @Override
    public YarnJob submit(){
        super.submit();
        /*
            Since main thread will stop after submitting, need to start a new thread for StreamSwitch.
        */
        startStreamSwitchOnNewThread();

        return this;
    }
}
