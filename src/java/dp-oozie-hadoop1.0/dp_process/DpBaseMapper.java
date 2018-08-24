package dp.dp_process;

/*
 *@author nkzhenhua@gmail.com
 *@version 1.0 on 2013-06-24
 *filename: DpBaseMapper.java
 *we should not use this DpBaseMapper directly
 *the basic Mapper for all kins of data process<br>
 * support configurable input feed parser and operator
 */
import dp.common.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

/**
 * this is the stats convert framework mapper class<br>
 * we should not use this DpBaseMapper directly<br>
 * the basic Mapper for all kins of data process<br>
 * support configurable input feed parser and operator<br>
 */
public class DpBaseMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, WritableComparable, Writable>
{
    protected DpIParser mStatsParser ;
    protected ArrayList<DpIBaseOperator> mOperatorList = new ArrayList<DpIBaseOperator>();

    public DpIParser getParser()
    {
        return mStatsParser;
    }
    public ArrayList<DpIBaseOperator> getOperator()
    {
        return mOperatorList;
    }

    public void map(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException
    {
    }
    /**
     * creat the operator list from JobConf
     * @param JobConf
     * dp.generator.num the number of generatoers
     * dp.generator.class.${i} the ith generator class name
     * dp.generator.filepattern.${i} the file name pattern against the operator
     * dp.generator.config.${i} the init configuration for init() 
     * @return void
     */
    public void createOperatorList(JobConf job, ArrayList<DpIBaseOperator> glist) throws  RuntimeException
    {
        String mapInputFile = job.get("map.input.file");
        //create the dimension/metric operator
        String generatorNum= job.get("dp.generator.num");
        int opNum= Integer.parseInt(generatorNum);
        int i=0;
        int gnum=0;
        for( ;i<opNum;i++ )
        {
            String gclass =job.get("dp.generator.class."+i);
            String filepattern=job.get("dp.generator.filepattern."+i);
            String confstr=job.get("dp.generator.config."+i);

            System.out.println("gclass is:"+gclass);
            System.out.println("filepattern is:"+filepattern);
            System.out.println("parameter is:"+confstr);
            if( gclass == null || filepattern == null ||
                    filepattern.equals("") || gclass.equals(""))
            {
                System.out.println("must set class path and input file pattern for generator:"+i);
                throw new RuntimeException("generator string format error: need 3 column");
            }
            String extPatten=".*"+filepattern+".*";
            if( mapInputFile.matches( extPatten ))
            {
                //current map input file matches the generator requirement
                gnum++;
                Class cl =null;
                DpIBaseOperator curGen =  null;
                try{
                    cl = Class.forName(gclass);
                    curGen = (DpIBaseOperator) cl.newInstance();
                }catch( Exception e)
                {
                    throw new RuntimeException("generator create instance error"+e.toString());
                }

                if( !curGen.init(job, confstr))
                {
                    System.out.println("init generator "+i+"error");
                    throw new RuntimeException("generator initial error");
                }
                glist.add(curGen);
            }
        }
        System.out.println("there are "+gnum+" generators for input file:"+ mapInputFile );
    }
    /**
     * instantiate input record parser class from the paramete in JobConf
     * the record parser maybe need be initialized with schema
     * @param JobConf , the job conf contain the user defined parser in "dp.parser" as file_patten:parser_class[;]. if not set , will use the DpDefaultParse instead.
     * @return DpIparser,  return the initiated record parser instance.
     * @exception parser initiated failed or class not found
     */
    public DpIParser createParser(JobConf job) throws RuntimeException
    {
        String curInputFile = job.get("map.input.file");
        String parserClassStr= job.get("dp.parser");
        DpIParser parser=null;
        if( parserClassStr != null && !parserClassStr.equals(""))
        {
            String [] parsers = parserClassStr.split(";");
            for( String parser_str : parsers )
            {
                if( parser_str == null || parser_str.equals(""))
                    continue;
                String [] one_parser_str = parser_str.split(":");
                if( one_parser_str.length != 2 )
                {
                    throw new RuntimeException("parser format error: filepatten:parser_class ");
                }
                if( curInputFile.matches(".*"+one_parser_str[0]+".*"))
                {
                    try{
                        Class classInst = Class.forName(one_parser_str[1]);
                        parser= (DpIParser) classInst.newInstance();
                    }catch (Exception e)
                    {
                        throw new RuntimeException("load parser class failed: ",e);
                    }
                    if( !parser.init(job))
                    {
                        throw new RuntimeException("init parser class failed: ");
                    }
                    break;
                }
            }
        }else
        {    
            try{
                Class classInst = Class.forName("dp.common.DpDefaultParser");
                parser= (DpIParser) classInst.newInstance();
            }catch (Exception e)
            {
                throw new RuntimeException("load parser class failed: ",e);
            }
        }
        return parser;
    }
    public void configure(JobConf job)
    {
        //create the parser class
        mStatsParser = createParser(job);
        //generate the generator list
        createOperatorList(job,mOperatorList);
    }
}
