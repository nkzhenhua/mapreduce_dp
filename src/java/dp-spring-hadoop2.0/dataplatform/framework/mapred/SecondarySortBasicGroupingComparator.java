package dataplatform.framework.mapred;


import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import dataplatform.framework.Constant.FrameworkLevelConstant;

/**
 * group the item with keyword. so the same keyword will be fed into the same reducer function
 * 
 * @author zhenhua
 *
 */
public class SecondarySortBasicGroupingComparator extends WritableComparator {
    final static Pattern REGX_PATTERN = Pattern.compile(FrameworkLevelConstant.KeyDelimiter);

    public SecondarySortBasicGroupingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable key1, WritableComparable key2) {
        // key pattern keyword\tsortId\ttimestamp
        Text str1 = (Text) key1;
        Text str2 = (Text) key2;
        String[] keyComp1 = REGX_PATTERN.split(new String(str1.getBytes()));
        String[] keyComp2 = REGX_PATTERN.split(new String(str2.getBytes()));
        return keyComp1[0].compareTo(keyComp2[0]);
    }
}
