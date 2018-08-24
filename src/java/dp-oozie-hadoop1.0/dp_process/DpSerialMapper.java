package dp.dp_process;

/*
 *@author nkzhenhua@gmail.com
 *@version 1.0 on 2013-06-24
 *filename: DpSerialBaseMapper.java
 * inherit form DpBaseMapper
 */
import dp.common.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.record.Buffer;

/**
 * inherit form DpBaseMapper, all the operators are processed serially against the same memory struct  MulMap
 */
public class DpSerialMapper extends DpBaseMapper implements Mapper<WritableComparable, Writable, WritableComparable, Writable>
{
    /**
     * over write DpBaseMapper and all the operatoer handle the same input record and output to the same MulMap
     */
    public void map(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException
    {
        mStatsParser.parse(key,value);
        WritableComparable handlerKey = mStatsParser.getKey();
        Writable handleValue = mStatsParser.getValue();
        TreeMap<String, Buffer> sField=new TreeMap<String, Buffer>();
        TreeMap<String, TreeMap<String, Buffer>> mField=new TreeMap<String, TreeMap<String, Buffer>>();
        TreeMap<String, ArrayList<TreeMap<String, Buffer>>> lField = new TreeMap<String, ArrayList<TreeMap<String, Buffer>>>();
        MulMap arecord= new MulMap(sField,mField,lField);
        for ( DpIBaseOperator curGen : mOperatorList )
        {
            curGen.process(handlerKey,handleValue,arecord,output,reporter);
        }
        lastOutput(handlerKey,arecord,output);
    }
    /**
     * output the record after all the operator processing, using the input record key as the default output key, if the
     * no "outputkey" in the MulMap record, user can rewrite the function
     * @param WritableComparable key, the original input key
     * @param MulMap record after the operators
     * @param OutputCollector<WritableComparable, Writable> output collect
     */
    public void lastOutput(WritableComparable key, MulMap record, OutputCollector<WritableComparable, Writable> output)
        throws IOException
    {
        //first get the output key from the use processed record
        Buffer keybuf=record.getSimpleFields().get("outputkey");
        if( null != keybuf)
        {
            try{
            String keystr = keybuf.toString("UTF-8");
            if( keystr != null )
            {
                output.collect(new Text(keystr.getBytes("UTF-8")), record);
                return;
            }
            }catch(Exception e)
            {
                System.out.println(e.toString());
            }
        }
        //no user defined key, use the input key
        output.collect(key,record);
    }
}
