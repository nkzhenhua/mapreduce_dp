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
 * @author:nkzhenhua@gmail.com
 */
public class DpStatsBaseReducer extends MapReduceBase implements Reducer<Text, Text, Text , Text>
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
        if (aggregator instanceof ValueHistogram) {

            String overall_val = aggregator.getReport();
            String detailed_val = ((ValueHistogram)aggregator).getReportDetails();
            String [] val1 = overall_val.split("\t");
            String [] val2 = detailed_val.split("\n");
            StringBuffer val = new StringBuffer();

            for (int i=0; i<val1.length; i++) {
                val.append(val1[i]).append(" ");
            }

            for (int i=0; i<val2.length; i++) {
                String [] nums = val2[i].split("\t");
                val.append(nums[1]).append(",").append(nums[2]).append(" ");
            }

            output.collect(new Text(keyStr), new Text(val.toString()));

        }
        else {
            String val = aggregator.getReport();
            output.collect(new Text(keyStr), new Text(val));
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
