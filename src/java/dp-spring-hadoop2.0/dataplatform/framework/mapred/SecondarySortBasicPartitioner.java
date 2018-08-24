package dataplatform.framework.mapred;

import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import seo.dataplatform.framework.Constant.FrameworkLevelConstant;

/**
 * partition class for composition key with only keyword, event the key is composited by keyword \t sortid \t timestamp
 * 
 * @author zhenhua
 *
 */
public class SecondarySortBasicPartitioner extends Partitioner<Text, Text> {
    final static Pattern REGX_PATTERN = Pattern.compile(FrameworkLevelConstant.KeyDelimiter);

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        // key pattern keyword\tsortId\ttimestamp, but partition with keyword
        String[] keyStr = REGX_PATTERN.split(new String(key.getBytes()));
        if (keyStr.length == FrameworkLevelConstant.PartitionKeyLen) {
            return Math.abs(keyStr[0].hashCode() % numReduceTasks);
        } else {
            System.out.println("***key format is not correct!**");
            return 0;
        }
    }
}
