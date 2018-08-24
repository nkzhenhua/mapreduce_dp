package dataplatform.springbean.outputlayer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * output layer interface
 * 
 * @author zhenhua
 *
 */
public interface OutputInterface {

    /**
     * get the name of this output
     * 
     * @return
     */
    public String getName();

    /**
     * each keyword data will be processed here. one keyword per time
     * 
     * @param inputRecord
     *            : the combined input record data from different data source, we shouldn't change the input data during
     *            process
     * @param outputRecord
     *            :all the temporary data during processing and new fields need to output to Keyword Record
     * @param mos
     *            : output stream
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("rawtypes")
    public void outputRow(KeywordAttribute inputRecord, KeywordAttribute outputRecord, MultipleOutputs mos)
            throws IOException, InterruptedException;

    /**
     * clean up function when all the data is processed
     * 
     * @param mos
     *            the output writer
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("rawtypes")
    public void cleanup(MultipleOutputs mos) throws IOException, InterruptedException;

    /**
     * this will be call before process any data. you can do some initial work here before process data
     * 
     * @param context
     *            the hadoop context contain job level info
     */
    @SuppressWarnings("rawtypes")
    public void init(Context context);
}
