package dataplatform.springbean.processor;

import java.util.List;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * This is the process layer of keyword repository framework. it will iterate every processor for each keyword User
 * could define new process in spring config file
 * 
 * @author zhenhua
 *
 */
public class SerializedOperatorPipeline {
    List<ProcessorInterface> processorPipeline;

    SerializedOperatorPipeline(List<ProcessorInterface> processorPipeline) {
        this.processorPipeline = processorPipeline;
    }

    public void process(KeywordAttribute inputRecord, KeywordAttribute outputRecord) {
        for (ProcessorInterface processor : processorPipeline) {
            processor.processRow(inputRecord, outputRecord);
        }
    }

    public void init() {
        for (ProcessorInterface processor : processorPipeline) {
            processor.init();
        }
    }

    public void clean() {
        for (ProcessorInterface processor : processorPipeline) {
            processor.clean();
        }
    }
}
