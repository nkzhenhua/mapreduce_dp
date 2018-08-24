package dataplatform.framework.mapred;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


import dataplatform.framework.Constant.FrameworkLevelConstant;
import dataplatform.framework.keyword.KeywordAttribute;
import dataplatform.framework.keyword.KeywordAttributeHelper;
import dataplatform.springbean.fileparser.GeneralTextFileParser;

/**
 * This is a Text file format common mapper, the input is Text File. the Content of each line will be the value and
 * output ( Keyword, Keyword attribute json string )
 * 
 * @author zhenhua
 *
 */
public class TextFileMapper extends Mapper<LongWritable, Text, Text, Text> {

    ApplicationContext springContext = null;

    GeneralTextFileParser parser;
    String currentProcessFileName;
    String runTime = null;
    String marketplaceId = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        /*
         * we should use spring config instead of brazil configure as much as possible. becasue the benifite of using
         * brazil is multiple config based on domain. we don't have apollo env
         */
        runTime = context.getConfiguration().get(FrameworkLevelConstant.RunTimeTag);
        springContext = new ClassPathXmlApplicationContext(FrameworkLevelConstant.SpirngConfigFile);

        // get input file name fed into this mapper
        InputSplit split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            try {
                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
            } catch (Exception e) {
                // wrap and re-throw error
                System.out.println("getInputSplit call exception");
                e.printStackTrace();
                throw new IOException();
            }
        } else {
            System.out.println("unknow input split!");
            throw new IOException("unknow input split!");
        }
        currentProcessFileName = fileSplit.getPath().getParent() + "/" + fileSplit.getPath().getName();
        System.out.println("input file name to process: " + currentProcessFileName);

        parser = (GeneralTextFileParser) springContext.getBean(FrameworkLevelConstant.GeneralTextKeywordParserBeanName);
        if (parser != null) {
            parser.init(context, currentProcessFileName);
        } else {
            throw new InterruptedException("GeneralTextFileParser init error!");
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        KeywordAttribute kwa = parser.parse(line);
        if (kwa == null) {
            return;
        }
        String kwAttrJsonStr = KeywordAttributeHelper.serializeKeywordAttribute(kwa);
        context.write(new Text(kwa.genKey()), new Text(kwAttrJsonStr));
    }

    @Override
    public void cleanup(Context context) {
    }
}
