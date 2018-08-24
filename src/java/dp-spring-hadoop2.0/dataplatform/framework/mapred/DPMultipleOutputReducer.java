package dataplatform.framework.mapred;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import dataplatform.framework.Constant.FrameworkLevelConstant;
import dataplatform.framework.Utils.KWRCustomerCounterTools;
import dataplatform.framework.keyword.DataSourceId;
import dataplatform.framework.keyword.KeywordAttribute;
import dataplatform.framework.keyword.KeywordAttributeHelper;
import dataplatform.framework.keyword.KeywordAttributeName;
import dataplatform.springbean.outputlayer.OutputManage;
import dataplatform.springbean.processor.SerializedOperatorPipeline;

/**
 * The reducer collect all keyword attributes from different data sources into a big keyword record, and call process
 * pipeline to do kinds of process. After one keyword was went through all the processors. Call the output manager to
 * output the related attribute to different output
 * 
 * @author  zohar
 *
 */
public class DPMultipleOutputReducer extends Reducer<Text, Text, Text, Text> {

    ApplicationContext springContext = null;
    MultipleOutputs mos;
    OutputManage outputManage;
    SerializedOperatorPipeline pipeline;
    String runTime;
    String marketplaceId;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        springContext = new ClassPathXmlApplicationContext(FrameworkLevelConstant.SpirngConfigFile);

        pipeline = (SerializedOperatorPipeline) springContext.getBean(FrameworkLevelConstant.OperationPipelineBeanName);
        pipeline.init(context);

        outputManage = (OutputManage) springContext.getBean(FrameworkLevelConstant.OutputManagerBeanName);
        outputManage.init(context);

        mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        KeywordAttribute inputRecord = mergeKwAttr(values);
        if (inputRecord == null) {
            // no kw source or kw record
            return;
        }
        // output recored will be the new keyword record with new timestamp
        KeywordAttribute outRecord = new KeywordAttribute(inputRecord.getKw(), DataSourceId.KWR.getSourceId(), runTime);
        outRecord.assignAttribute(inputRecord);

        pipeline.process(inputRecord, outRecord);
        outputManage.process(inputRecord, outRecord, mos);
    }

    public KeywordAttribute mergeKwAttr(Iterable<Text> values) throws IOException, InterruptedException {
        // the first must be keyword record or tommy query, or lest return
        Iterator<Text> it = values.iterator();

        String recStr = new String(it.next().getBytes());
        KeywordAttribute firstKwAttr = KeywordAttributeHelper.deserializeKeywordAttribute(recStr);
        if (firstKwAttr == null) {
            return null;
        }
        if (firstKwAttr.getSId() > DataSourceId.MUST_HAVE.getSourceId()) {
            // missing keyword record or tommy data.
            System.out.println("there is no tommy or kwrecord, discard all the attributes for:" + firstKwAttr.getKw());
            return null;
        }
        KeywordAttribute inputRecord = null;
        if (firstKwAttr.getSId() != DataSourceId.KWR.getSourceId()) {
            // add the first timestamp of adding to kwr
            inputRecord = new KeywordAttribute(firstKwAttr.getKw(), DataSourceId.KWR.getSourceId(), runTime);
            inputRecord.assignAttribute(firstKwAttr);
            inputRecord.setSimpleFieldItem(KeywordAttributeName.KW_FIRST_ADDING_TIME.getAttrName(), runTime);
        } else {
            inputRecord = firstKwAttr;
        }
        while (it.hasNext()) {
            recStr = new String(it.next().getBytes());
            // the kwattrs are orded by DataSourceId_timestamp
            KeywordAttribute kwAttr = KeywordAttributeHelper.deserializeKeywordAttribute(recStr);
            inputRecord.assignAttribute(kwAttr);
        }
        return inputRecord;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        pipeline.clean();
        outputManage.cleanup(mos);
        // output metrics here
        mos.close();
    }
}
