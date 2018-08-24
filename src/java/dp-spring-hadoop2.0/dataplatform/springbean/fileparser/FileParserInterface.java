package dataplatform.springbean.fileparser;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * input layer
 * 
 * @author zhenhua
 *
 */
public interface FileParserInterface {
    public void init(Context context, String currentFile);
    public KeywordAttribute parse(String input);
}
