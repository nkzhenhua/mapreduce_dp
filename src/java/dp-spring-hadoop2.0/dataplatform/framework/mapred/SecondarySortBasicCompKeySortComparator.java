package dataplatform.framework.mapred;

import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import dataplatform.framework.Constant.FrameworkLevelConstant;

/**
 * sort the values of reducer by keyword then sortId then timestamp when we iterate the reduce values
 * 
 * @author zhenhua
 *
 */
public class SecondarySortBasicCompKeySortComparator extends WritableComparator {
    final static Pattern REGX_PATTERN = Pattern.compile(FrameworkLevelConstant.KeyDelimiter);

    public SecondarySortBasicCompKeySortComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable str1, WritableComparable str2) {
        // key pattern keyword\tsortId\ttimestamp
        Text key1 = (Text) str1;
        Text key2 = (Text) str2;
        String[] keyComp1 = REGX_PATTERN.split(new String(key1.getBytes()));
        String[] keyComp2 = REGX_PATTERN.split(new String(key2.getBytes()));

        int cmpResult = keyComp1[0].compareTo(keyComp2[0]);
        if (cmpResult == 0)// same keyword
        {
            // compare the sortId
            int sortId1 = Integer.parseInt(keyComp1[1]);
            int sortId2 = Integer.parseInt(keyComp2[1]);
            if (sortId1 != sortId2) {
                return sortId1 < sortId2 ? -1 : 1;
            }
            // compare timestamp
            return keyComp1[2].compareTo(keyComp2[2]);
        }
        return cmpResult;
    }
}
