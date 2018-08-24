package dataplatform.springbean;

import java.util.Map;

/**
 * initial the map reduce job configuration through the spring bean, we don't need to write the driver every time.
 * 
 * @author zhenhua
 *
 */
public class MRJobInitConfBean {
    // Map<jobname, jobConf<key,value>>
    private Map<String, Map<String, String>> jobConfigProperties;

    public MRJobInitConfBean(Map<String, Map<String, String>> jobConfigProperties) {
        this.jobConfigProperties = jobConfigProperties;
    }

    public Map<String, String> getJobConfigProperties(String jobName) {
        return this.jobConfigProperties.get(jobName);
    }
}
