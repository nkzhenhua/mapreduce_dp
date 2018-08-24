package dataplatform.framework.keyword;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * keyword related function helper
 * 
 * @author zhenhua
 *
 */
public class KeywordAttributeHelper {

    public static KeywordAttribute deserializeKeywordAttribute(String kwAttrStr) {
        if (kwAttrStr == null || kwAttrStr.length() == 0) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(kwAttrStr, new TypeReference<KeywordAttribute>() {
            });
        } catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static String serializeKeywordAttribute(KeywordAttribute kwAttr) {
        if (kwAttr == null) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(kwAttr);
        } catch (JsonGenerationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
