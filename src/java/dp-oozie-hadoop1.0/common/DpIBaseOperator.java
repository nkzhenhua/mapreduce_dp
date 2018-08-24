package dp.common;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

/**
 *@author nkzhenhua@gmail.com
 * the operator interface for all kinds of processing
 */
public interface DpIBaseOperator
{
    /**
     *init the operator
     *@param JobConf the jobconf 
     *@param String param the extral init configuration
     *@return boolean if init successfully
     */
    public boolean init(JobConf job, String param);

    /**
     *  the independent process interface, all processor has its own output
     *  @param WritableComparable key
     *  @param Writable value 
     *  @param OutputCollector<WritableComparable, Writable> output
     *  @param Reporter reporter
     *  @return void
     */
    public void process(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException;

     /**
     *  the process serial interface, all the processor based on the same output record MulMap
     *  @param WritableComparable key
     *  @param Writable value 
     *  @param OutputCollector<WritableComparable, Writable> output
     *  @param Reporter reporter
     *  @return void
     */
    public void process(WritableComparable key, Writable value, MulMap arecord, OutputCollector<WritableComparable, Writable> output,Reporter reporter) throws IOException;
}
