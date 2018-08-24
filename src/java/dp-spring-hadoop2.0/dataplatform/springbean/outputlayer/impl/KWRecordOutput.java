package dataplatform.springbean.outputlayer.impl;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataplatform.framework.keyword.KeywordAttribute;
import dataplatform.framework.keyword.KeywordAttributeHelper;
import dataplatform.framework.keyword.KeywordRecord;
import dataplatform.springbean.outputlayer.OutputInterface;

/**
 * specific output implementation
 * 
 * @author zhenhua
 *
 */

public class KWRecordOutput implements OutputInterface {
    String outputName;
    String subDirAndName;

    KWRecordOutput(String outputName, String subDirAndName) {
        this.outputName = outputName;
        this.subDirAndName = subDirAndName;
    }

    @Override
    public String getName() {
        return this.outputName;
    }

    @Override
    public void outputRow(KeywordAttribute inputRecord, KeywordAttribute outputRecord, MultipleOutputs mos)
            throws IOException, InterruptedException {
        KeywordRecord kwr = (KeywordRecord) inputRecord;
        mos.write(this.outputName, new Text(KeywordAttributeHelper.serializeKeywordAttribute(kwr)), NullWritable.get(),
                this.subDirAndName);
    }
}
