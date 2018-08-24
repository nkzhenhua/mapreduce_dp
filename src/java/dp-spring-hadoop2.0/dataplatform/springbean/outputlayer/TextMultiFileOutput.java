package dataplatform.springbean.outputlayer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * this class can't be used with new api, if is MultipleTextOutputFormat, you must use JobConf instead of Job. keep
 * these class here as a learning material
 * 
 * @author zhenhua
 *
 */
public class TextMultiFileOutput extends MultipleTextOutputFormat<Text, LongWritable> {
    @Override
    protected String generateFileNameForKeyValue(Text key, LongWritable value, String name) {
        String keys[] = key.toString().split("#");
        if (keys.length >= 2) {
            return keys[0] + "_" + name;
        }
        return name;
    }

    @Override
    protected Text generateActualKey(Text key, LongWritable value) {
        String keystr = key.toString();
        String keys[] = keystr.split("#");

        if (keys.length <= 1) {
            return key;
        }

        StringBuffer buf = new StringBuffer();
        buf.append(keys[1]);
        for (int i = 2; i < keys.length; i++) {
            buf.append('\t');
            buf.append(keys[i]);
        }
        return new Text(buf.toString());
    }
}
