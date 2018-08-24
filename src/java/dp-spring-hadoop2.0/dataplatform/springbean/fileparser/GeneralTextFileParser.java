package dataplatform.springbean.fileparser;

import java.util.Map;

import dataplatform.framework.keyword.KeywordAttribute;

/**
 * framework input layer. \n General Text File parser. it will call related data source parser according to the file
 * path stored in the mapping Map<String, FileParserInterface>, the specific file parser will generate different keyword
 * attributes records
 * 
 * @author zhenhua
 *
 */
public class GeneralTextFileParser {
    // <file path patten, specific parser>
    Map<String, FileParserInterface> txtInputParsers;

    // parser for current input block( one mapper only process one block)
    FileParserInterface currentParser;
    String inputFile;

    public GeneralTextFileParser(Map<String, FileParserInterface> txtInputParsers) {
        this.txtInputParsers = txtInputParsers;
    }

    @SuppressWarnings("rawtypes")
    public void init(Context context, String currentFile) {
        this.inputFile = currentFile;
        for (String matcher : this.txtInputParsers.keySet()) {
            // use the String.matches which use Regular Expression for the whole path
            if (currentFile.matches(FrameworkLevelConstant.RegexWildMatch + matcher
                    + FrameworkLevelConstant.RegexWildMatch)) {
                currentParser = this.txtInputParsers.get(matcher);
                currentParser.init(context, currentFile);
            }
        }

    }

    public KeywordAttribute parse(String input) {
        if (currentParser != null) {
            return currentParser.parse(input);
        }
        System.out.println("File Parser is not initialed for input file: " + this.inputFile);
        return null;
    }
}
