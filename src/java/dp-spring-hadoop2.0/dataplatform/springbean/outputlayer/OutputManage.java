package dataplatform.springbean.outputlayer;

import java.util.List;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * framework output layer.\t For current keyword attribues, generate multiple outputs from the spring config file by
 * call different output
 * 
 * @author zhenhua
 *
 */
public class OutputManage {
    List<OutputInterface> multiOutput;

    OutputManage(List<OutputInterface> multiOutput) {
        this.multiOutput = multiOutput;
    }

    @SuppressWarnings("rawtypes")
    public void init(Context context) {
        for (OutputInterface outputProcessor : multiOutput) {
            outputProcessor.init(context);
        }
    }

    @SuppressWarnings("rawtypes")
    public void cleanup(MultipleOutputs mos) {
        for (OutputInterface outputProcessor : multiOutput) {
            try {
                outputProcessor.cleanup(mos);
            } catch (Exception e) {
                System.out.println("exception when call cleanup for:" + outputProcessor.getName() + ":\n"
                        + e.toString());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public void process(KeywordAttribute inputRecord, KeywordAttribute outputRecord, MultipleOutputs mos) {
        for (OutputInterface outputProcessor : multiOutput) {
            try {
                outputProcessor.outputRow(inputRecord, outputRecord, mos);
            } catch (Exception e) {
                System.out.println(outputProcessor.getName() + " exception when output:" + inputRecord.getKw() + ":"
                        + e.toString());
            }
        }
    }
}
