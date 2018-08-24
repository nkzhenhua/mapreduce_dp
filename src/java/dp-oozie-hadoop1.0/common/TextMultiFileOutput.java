package dp.common;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import java.lang.String;


public class TextMultiFileOutput extends MultipleTextOutputFormat<Text, Text>
{
    protected String generateFileNameForKeyValue(Text key, Text value,String name)
    {
        String keys[]=key.toString().split("#");
        if( keys.length >= 2 )
        {
            return keys[0]+"_"+name;
        }
        return name;
    }
    protected Text generateActualKey(Text key, Text value)
    {
        String keystr=key.toString();
        String keys[]=keystr.split("#");

        if (keys.length <= 1)
        {
            return key;
        }

        StringBuffer buf = new StringBuffer();
        buf.append(keys[1]);
        for (int i = 2; i < keys.length; i++)
        {
            buf.append('\t');
            buf.append(keys[i]);
        }
        return new Text(buf.toString());
    }
}
