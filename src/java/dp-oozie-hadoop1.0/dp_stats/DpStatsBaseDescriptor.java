package dp.dp_stats;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregator;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorBaseDescriptor;

/**
 * @author nkzhenhua@gmail.com
 * user defined Descriptor and valueaggregator
 *
 */
public class DpStatsBaseDescriptor
    extends ValueAggregatorBaseDescriptor {

    private static final Map<String, Class<? extends ValueAggregator>>
    g_registered = new TreeMap<String, Class<? extends ValueAggregator>>();

    /*
     * STEP 1
     *
     * First add static string representing new aggregator class, for example
     * static public final String MY_NEW_STAT = "MyNewStat";
     * where, MyNewStat is the literal name of new class
     */
    static public final String STRING_VALUE_UNIQ = "STRING_VALUE_UNIQ";

    /*
     * STEP 2
     *
     * Next, register the string with the aggregator class
     */
    static {
        register(STRING_VALUE_UNIQ, StringValueUniq.class);
    }

    /**
     *
     * @param type
     * @param clazz
     * @return
     */
    public static final boolean register(String type, Class<? extends ValueAggregator> clazz) {
        return null == g_registered.put(type.toLowerCase(), clazz);
    }

    /**
     *
     * @param type
     * @return
     */
    public static Class<? extends ValueAggregator> registered(String type) {
        return g_registered.get(type.toLowerCase());
    }

    /**
     * This method is equivalent of generateValueAggregator of super class.
     * It facilitates instantiation of new aggregator objects, including
     * those defined here
     */
    public static ValueAggregator generateValueAggregator(String type) {
        final ValueAggregator va = ValueAggregatorBaseDescriptor.generateValueAggregator(type);
        return null != va ? va : generateStatsAggregator(type);
    }

    /**
     *
     * @param type
     * @return
     */
    public static ValueAggregator generateStatsAggregator(String type) {
        final Class<? extends ValueAggregator> clazz = registered(type);
        if (null == clazz){
            System.out.println("generate user defined ValueAggregator for "+type+" error");
            return null;
        }

        try {
            return clazz.newInstance();
        }
        catch (Exception e) {
            System.out.println("init user defined ValueAggregator for "+type+" error:"+e.toString());
            throw new RuntimeException("Can't instantiate ValueAggregator: " + clazz, e);
        }
    }

    /**
     * Must call configure of superclass
     */
    public void configure(JobConf job) {
        super.configure(job);
    }
}
