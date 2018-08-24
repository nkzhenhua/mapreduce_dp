package dp.common;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

/**
* @author nkzhenhua@gmail.com 
* parse the input record and provide the key and value
* */
public interface DpIParser
{
    public boolean init(JobConf job);
    public void parse(WritableComparable key, Writable value) throws IOException;
    public WritableComparable getKey();
    public Writable getValue();
}
