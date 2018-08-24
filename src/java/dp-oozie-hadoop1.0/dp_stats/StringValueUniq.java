package dp.dp_stats;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.mapred.lib.aggregate.ValueAggregator;

/**
 * This class unique all the string
 * This is useful feature for shred combiner or multi-pass stats.
 *
 * @author zzh
 *
 */
public class StringValueUniq implements ValueAggregator {

    HashSet<String> mValue= new HashSet<String>();
    /**
     * The default constructor
     *
     */
    public StringValueUniq() {
        reset();
    }

    /**
     * add a value to the aggregator
     *
     * @param val
     *          an object whose string representation represents value.
     *
     */
    public void addNextValue(Object val) {
        String valstr=val.toString();
        String vals[]=valstr.split(";");
        for(String oneval:vals)
        {
            mValue.add(oneval);
        }
    }

    /**
     * @return the string representation of the aggregated value
     */
    public String getReport() {
        String retv = "";
        if (!mValue.isEmpty())
        {
            for(String val:mValue)
            {
                if(retv.equals(""))
                {
                    retv=val;
                }else{
                    retv+=";"+val;
                }
            }
        }
        return retv;
    }

    /**
     * reset the aggregator
     */
    public void reset() {
        mValue.clear();
    }

    /**
     * @return return an array of one element. The element is a string
     *         representation of the aggregated value. The return value is
     *         expected to be used by the a combiner.
     */
    public ArrayList<String> getCombinerOutput() {

        ArrayList<String> retv = new ArrayList<String>(mValue.size());
        if (!mValue.isEmpty())
        {
            for(String val:mValue)
            {
                retv.add(val);
            }
        }
        return retv;
    }
}
