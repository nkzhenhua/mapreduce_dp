package dataplatform.framework.Driver;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import dataplatform.framework.Constant.FrameworkLevelConstant;
import dataplatform.springbean.MRJobInitConfBean;


/**
 * This is the entrance of a map-reduce job, A common Map-Reduce job Driver. The job properties are define in a spring
 * configure file Including: the Map/Reducer classes, reduce numbers ...
 * 
 * This common driver will read the config file from xml file and replace some placeholder for runtime information here
 * like running date time, marketplace which make the job more extendible
 * 
 * run command: hadoop jar ./process.jar dataplatform.framework.Driver.CommonJobDriver
 * -libjars ${} -archives test.tgz -conf <configuration file> -D<property=value> multipleOutputJobDemo ${runtime} others like [ pattern=replace] eg:
 * 
 */
public class CommonJobDriver extends Configured implements Tool {

    String runJobName;
    String runTime;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        if (initJobConf(job, args) == false) {
            System.out.println("init job failed");
            printHelp();
            return 1;
        }
        boolean succeed = job.waitForCompletion(true);
        // write metrics to CloudWatch
        if (succeed) {
            Counters counters = job.getCounters();
            if (counters != null) {
                for (CounterGroup counterGroup : counters) {
                    for (Counter counter : counterGroup) {
                        String metricName = counterGroup.getDisplayName() + '/' + counter.getDisplayName();
                        // replace space into _ in metricName according to name
                        // convention of pmet
                        System.out.println(metricName + ":" + counter.getValue());
                    }
                }
            }
        }
        return succeed ? 0 : 1;
    }

    public boolean initJobConf(Job job, String[] args) throws IOException {

        job.setJarByClass(getClass());

        GenericOptionsParser gop = new GenericOptionsParser(job.getConfiguration(), args);
        String[] remainArgs = gop.getRemainingArgs();
        if (remainArgs.length < 5) {
            return false;
        }

        runJobName = remainArgs[0];
        runTime = remainArgs[1];

        String jobName = runJobName + "_" + runTime;
        job.setJobName(jobName);

        System.out.println("remain args:" + Arrays.toString(remainArgs));
        /*
         * job.addCacheFile(new URI("/user/yourname/cache/some_file.json#dir"));
         */

        // set system env before render Spring bean and hadoop config
        StringBuffer spingProperty = new StringBuffer();
        for (int i = 4; i < remainArgs.length; i++) {
            spingProperty.append(remainArgs[i]).append(FrameworkLevelConstant.SystemPropertyItemDelimiter);
            if (remainArgs[i].contains(FrameworkLevelConstant.SystemPropertyDelimiter)) {
                String[] kv = remainArgs[i].split(FrameworkLevelConstant.SystemPropertyDelimiter);
                System.setProperty(kv[0], kv[1]);
                job.getConfiguration().set(kv[0], kv[1]);
            }
        }
        /**
         * seems the system property only takes effect in hadoop configuration. not consumed by Spring bean variable(
         * this could be done in the feature if we need that in spring bean)
         */
        System.setProperty(FrameworkLevelConstant.RunTimeTag, runTime);

        System.getProperties().list(System.out);

        @SuppressWarnings("resource")
        ApplicationContext springContext =
                new ClassPathXmlApplicationContext(FrameworkLevelConstant.SpirngConfigFile);
        MRJobInitConfBean mrJobConf =
                (MRJobInitConfBean) springContext.getBean(FrameworkLevelConstant.JobConfigPropertiesBeanName);
        for (Map.Entry<String, String> entry : mrJobConf.getJobConfigProperties(runJobName).entrySet()) {
            // setup the map-red job configuration
            job.getConfiguration().set(entry.getKey(), entry.getValue());
            System.out.println("key:" + entry.getKey() + " value:" + entry.getValue());
        }

        job.getConfiguration().set(FrameworkLevelConstant.RunTimeTag, runTime);
        job.getConfiguration().set(FrameworkLevelConstant.SpringProperty, spingProperty.toString());

        if (job.getConfiguration().get("mapreduce.output.lazyoutputformat.outputformat") != null) {
            Class<? extends OutputFormat> opf;
            try {
                opf =
                        (Class<? extends OutputFormat>) Class.forName(job.getConfiguration().get(
                                "mapreduce.output.lazyoutputformat.outputformat"));
                LazyOutputFormat.setOutputFormatClass(job, opf);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                System.out.println("get class from name error:"
                        + job.getConfiguration().get("mapreduce.output.lazyoutputformat.outputformat"));
                e.printStackTrace();
            }

        }

        Iterator<Map.Entry<String, String>> it = job.getConfiguration().iterator();
        System.out.println("output all the hadoop job configruation to terminal for debugging:\n");
        while (it.hasNext()) {
            Map.Entry<String, String> item = it.next();
            System.out.println(item.getKey() + ":" + job.getConfiguration().get(item.getKey()));
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new CommonJobDriver(), args);
        System.exit(rc);
    }
}