package dataplatform.framework.utils;
/**
 * this will provide the common tool to add a counter like:
 * CustomerCounterTools.increaseCounter(CustomerCounterName.Name,1); and the counter will be published automatically after the job is done
 * 
 * @author zhenhua
 *
 */
public class CustomerCounterTools {
    static HashMap<CustomerCounterName, Long> counters = new HashMap<CustomerCounterName, Long>();

    public static void increaseCounter(CustomerCounterName counter, long unit) {
        counters.put(counter, counters.getOrDefault(counter, 0L) + unit);
    }

    // overload the publishCoounters for calling in mapper or reducer
    @SuppressWarnings("rawtypes")
    public static void publishCounters(org.apache.hadoop.mapreduce.Mapper.Context context) {
        for (Map.Entry<CustomerCounterName, Long> entry : counters.entrySet()) {
            context.getCounter(entry.getKey()).increment(entry.getValue());
        }
    }

    @SuppressWarnings("rawtypes")
    public static void publishCounters(org.apache.hadoop.mapreduce.Reducer.Context context) {
        for (Map.Entry<CustomerCounterName, Long> entry : counters.entrySet()) {
            context.getCounter(entry.getKey()).increment(entry.getValue());
        }
    }
}
