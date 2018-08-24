package dp.dp_stats;

import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.mapred.lib.aggregate.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorBaseDescriptor;
/**
 * @author nkzhenhua@gmail.com
 */
public class DpStatsBaseCombiner extends MapReduceBase implements Reducer<Text, Text, Text , Text>
{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text , Text> output, Reporter reporter) throws IOException
    {
        String keyStr = getKey(key) ;
        ValueAggregator aggregator = getAggregator(key) ;
        if( aggregator == null)
        {
            System.out.println("no aggregator for "+ key.toString());
            return;
        }

        while (values.hasNext()) {
            //add next value to stat aggregator to stats can be computed
            aggregator.addNextValue(values.next());
        }
        Iterator outputs = aggregator.getCombinerOutput().iterator();
        while (outputs.hasNext()) {
            Object v = outputs.next();
            if (v instanceof Text) {
                output.collect(key, (Text)v);
            } else {
                output.collect(key, new Text(v.toString()));
            }
        }
    }
    public void configure(JobConf job)
    {
    }
    protected String getKey(Text key) {
        String keyStr = key.toString();
        return keyStr.substring(keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR)+ ValueAggregatorDescriptor.TYPE_SEPARATOR.length());
    }
    protected ValueAggregator getAggregator(Text key) {
        String type = getAggregatorType(key) ;
        return DpStatsBaseDescriptor.generateValueAggregator(type);
    }
    protected String getAggregatorType(Text key) {
        String keyStr = key.toString();
        return keyStr.substring(0, keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR)) ;
    }
}
