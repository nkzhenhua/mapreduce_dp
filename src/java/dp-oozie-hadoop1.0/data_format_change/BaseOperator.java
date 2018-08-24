package dp.data_format_change;

import dp.common.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;

import org.apache.hadoop.record.Buffer;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.ArrayList;
/**
 *@author nkzhenhua@gmail.com
 * this is the base class Generator for extracting metric info into MulMap,
 * and emit to reducei
 */
class BaseOperator implements DpIBaseOperator
{
	//switch for this Operator
	protected boolean generatorOn=true;

	protected MulMap mMetric = null;
	protected MulMap nulltmp= new MulMap(new TreeMap<String, Buffer>(), 
			new TreeMap<String, TreeMap<String, Buffer>>(), 
			new TreeMap<String, ArrayList<TreeMap<String, Buffer>>>());

	public boolean init(JobConf job, String param)
	{
		return true;
	}
	/**
	 * emit the 0 , tell the reduce to start(no actual usefulness)
	 */
	public void begin(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter)
	{
		try {
		output.collect(new Text("0"),nulltmp);
		}catch (Exception e)
		{
			System.out.println("an exception in begin():"+e.toString());
		}
	}
	/**
	 * emit the 2 , tell the reduce all the record disposed, should start to conver to json and output the json result 
	 */
	public void end(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter)
	{
		try{
			output.collect(new Text("2"),nulltmp);
		}catch (Exception e)
		{
			System.out.println("an exception in end"+e.toString());
		}
	}

	/**
	 * if this recored should be filtered
	 */
	public boolean filter(WritableComparable key, Writable value)
	{
		return false;
	}
	/**
	 * fill the info into a MulMap memory struct
	 */
	public void process_metric(WritableComparable key, Writable value,MulMap output,Reporter reporter)
	{
	}
	public void emit(MulMap Metric, OutputCollector<WritableComparable, Writable> output) throws IOException
	{
	}
	/*
	 *called by map() for every record
	 */
	public void process(WritableComparable key, Writable value, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException
	{
		if( generatorOn )
		{
			mMetric = new MulMap(new TreeMap<String, Buffer>(), 
					new TreeMap<String, TreeMap<String, Buffer>>(), 
					new TreeMap<String, ArrayList<TreeMap<String, Buffer>>>());
			begin(key,value,output,reporter);
			process_metric(key,value,mMetric,reporter);
			emit(mMetric,output);
			end(key,value,output,reporter);
		}
	}

	public void process(WritableComparable key, Writable value, MulMap record, OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException
    {
    }
}
