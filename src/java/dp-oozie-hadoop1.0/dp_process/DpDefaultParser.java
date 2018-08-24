package dp.dp_process;

import dp.common.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

/**
* @author nkzhenhua@gmail.com 
* parse the input record and provide the key and value
* */
public class DpDefaultParser implements DpIParser
{
    private WritableComparable _key;
    private Writable _value;
    public boolean init(JobConf job)
    {
        return true;
    }
    public void parse(WritableComparable key, Writable value) throws IOException
    {
        _key=key;
        _value=value;
    }
    public WritableComparable getKey()
    {
        return _key;
    }
    public Writable getValue()
    {
        return _value;
    }
}
