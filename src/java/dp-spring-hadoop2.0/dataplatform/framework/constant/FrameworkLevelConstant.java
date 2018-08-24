package dataplatform.framework.Constant;

/**
 * define the framework level constants here
 * 
 * @author zhenhua
 *
 */
public class FrameworkLevelConstant {
    public static final String SpirngConfigFile =
            "dataplatform/springbean/spring-configuration/application-config.xml";
    public static final String GeneralTextKeywordParserBeanName = "textFileParser";
    public static final String JobConfigPropertiesBeanName = "mrJobConfigProperties";
    public static final String OperationPipelineBeanName = "serializedOperatorPipeline";
    public static final String OutputManagerBeanName = "outputManage";
    public static final String KeyDelimiter = "\t";
    public static final String RegexWildMatch = ".*";
    public static final String RunTimeTag = "RUNTIME";

    public static final String SystemPropertyItemDelimiter = "##";
    public static final String SystemPropertyDelimiter = "=";
    public static final String SpringProperty = "SpringProperty";

    public static final int PartitionKeyLen = 3;
}
