package dp.dp_process;

/*
 *@author nkzhenhua@gmail.com
 *@version 1.0 on 2013-06-24
 *filename: DpParallelBaseMapper.java
 *the basic Mapper for all kins of data process<br>
 * support configurable input feed parser and operator
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

/**
 * inherit form DpBaseMapper, all the operators are processed indepently 
 */
public class DpParallelMapper extends DpBaseMapper implements Mapper<WritableComparable, Writable, WritableComparable, Writable>
{
    /**
     * over write DpBaseMapper and all the operatoer handle the same input record and output independently
     */
      public void map(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException
    {
        mStatsParser.parse(key,value);
        WritableComparable handlerKey = mStatsParser.getKey();
        Writable handleValue = mStatsParser.getValue();
        for ( DpIBaseOperator curGen : mOperatorList )
        {
            curGen.process(handlerKey,handleValue,output,reporter);
        }
    }
}
