package org.apache.samza.job.dm;

import org.apache.samza.config.Config;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;
import org.apache.samza.util.Logging;

public class DMJobFactory implements StreamJobFactory {
    @Override
    public DMJob getJob(Config config) {

        return new DMJob(config);
    }
}
