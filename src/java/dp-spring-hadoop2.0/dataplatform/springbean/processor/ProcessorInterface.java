package dataplatform.springbean.processor;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * process layer
 * 
 * @author zhenhua
 *
 */
public class SerializedOperatorPipeline {
    List<ProcessorInterface> processorPipeline;

    SerializedOperatorPipeline(List<ProcessorInterface> processorPipeline) {
        this.processorPipeline = processorPipeline;
    }

    /**
     * iterate the processor with one keyword record which contain all the attribute in inputRecord
     * 
     * @param inputRecord
     * @param outputRecord
     */
    public void process(KeywordAttribute inputRecord, KeywordAttribute outputRecord) {
        for (ProcessorInterface processor : processorPipeline) {
            try {
                processor.processRow(inputRecord, outputRecord);
            } catch (Exception e) {
                System.out.println("procesor process exception for " + processor.getClass().getName() + e.toString());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public void init(Context context) {
        for (ProcessorInterface processor : processorPipeline) {
            try {
                processor.init(context);
            } catch (Exception e) {
                System.out.println("processorpipeline: " + processor.getClass().getName()
                        + " init failed, skip to next one!" + e.toString());
            }
        }
    }

    public void clean() {
        for (ProcessorInterface processor : processorPipeline) {
            try {
                processor.clean();
            } catch (Exception e) {
                System.out.println("processorpipeline: " + processor.getClass().getName()
                        + " clean failed, skip to next one!" + e.toString());
            }
        }
    }
}
