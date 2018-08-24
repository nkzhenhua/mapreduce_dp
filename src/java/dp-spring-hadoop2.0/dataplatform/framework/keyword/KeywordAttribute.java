package dataplatform.framework.keyword;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import lombok.Getter;
import lombok.Setter;

import org.codehaus.jackson.annotate.JsonIgnore;

import dataplatform.framework.Constant.FrameworkLevelConstant;

/**
 * 
 * @author zohar 
 *
 */

public class KeywordAttribute {

    /**
     * keyword
     */
    @Getter
    @Setter
    String kw;

    /*
     * this filed used for sorting in reducer to make the different keyword attribute source in a specific order it
     * could be empty
     */
    @Getter
    @Setter
    int sId;

    // the timestamp of data feed
    @Getter
    @Setter
    String tm;

    public KeywordAttribute(int orderId) {
        this.sId = orderId;
    }

    public KeywordAttribute() {
    }

    public KeywordAttribute(String keyword) {
        this.kw = keyword;
        this.sId = DataSourceId.KWR.getSourceId();
    }

    public KeywordAttribute(String keyword, int orderId) {
        this.kw = keyword;
        this.sId = orderId;
    }

    public KeywordAttribute(String keyword, int orderId, String timestamp) {
        this.kw = keyword;
        this.sId = orderId;
        this.tm = timestamp;
    }

    @Getter
    @Setter
    TreeMap<String, String> sField = new TreeMap<String, String>();
    @Getter
    @Setter
    TreeMap<String, TreeMap<String, String>> mField = new TreeMap<String, TreeMap<String, String>>();
    @Getter
    @Setter
    TreeMap<String, Set<String>> uField = new TreeMap<String, Set<String>>();
    @Getter
    @Setter
    TreeMap<String, TreeMap<String, TreeMap<String, String>>> nField =
            new TreeMap<String, TreeMap<String, TreeMap<String, String>>>();
    /*
     * this field could be used as temporal memory storage when do computation. it will not be serialized into json
     * string. Uncomment this annotation when deploy to production to reduce the output size
     * 
     * add @Getter and @Setter and remove the @JsonIgnor, the dynamicMap will be output the KWR for debugging, but be
     * sure to remove them for prod data
     * 
     * @Getter
     * 
     * @Setter
     */
    @JsonIgnore
    Map<String, Object> dynamicMap = new HashMap<String, Object>();

    public void setTempValue(String key, Object value) {
        this.dynamicMap.put(key, value);
    }

    public Object getTempValue(String key) {
        return this.dynamicMap.get(key);
    }

    public void setSimpleFieldItem(String key, String value) {
        sField.put(key, value);
    }

    public String getSimpleFieldItem(String key) {
        return sField.get(key);
    }

    public void addSetFieldItem(String key, String value) {
        Set<String> setValue = uField.get(key);
        if (setValue == null) {
            setValue = new HashSet<String>();
        }
        setValue.add(value);
        uField.put(key, setValue);
    }

    public Set<String> getSetFieldItem(String key) {
        Set<String> setValue = uField.get(key);
        return setValue;
    }

    public void setMapFieldItem(String key1, String key2, String value) {
        TreeMap<String, String> firstLevelMap = mField.get(key1);
        if (firstLevelMap == null) {
            firstLevelMap = new TreeMap<String, String>();
            mField.put(key1, firstLevelMap);
        }
        firstLevelMap.put(key2, value);
    }

    public String getMapFieldItem(String key1, String key2) {
        TreeMap<String, String> firstLevelMap = mField.get(key1);
        if (firstLevelMap == null) {
            return null;
        }
        return firstLevelMap.get(key2);
    }

    public void setNestFieldItem(String key1, String key2, String key3, String value) {
        TreeMap<String, TreeMap<String, String>> firstLevelMap = nField.get(key1);
        if (firstLevelMap == null) {
            firstLevelMap = new TreeMap<String, TreeMap<String, String>>();
            nField.put(key1, firstLevelMap);
        }
        TreeMap<String, String> secondLevelMap = firstLevelMap.get(key2);
        if (secondLevelMap == null) {
            secondLevelMap = new TreeMap<String, String>();
            firstLevelMap.put(key2, secondLevelMap);
        }
        secondLevelMap.put(key3, value);
    }

    public String getNestFieldItem(String key1, String key2, String key3) {
        TreeMap<String, TreeMap<String, String>> firstLevelMap = nField.get(key1);
        if (firstLevelMap == null) {
            return null;
        }
        TreeMap<String, String> secondLevelMap = firstLevelMap.get(key2);
        if (secondLevelMap == null) {
            return null;
        }
        return secondLevelMap.get(key3);
    }

    public String genKey() {
        StringBuffer keyBuf = new StringBuffer();
        return keyBuf.append(this.kw).append(FrameworkLevelConstant.KeyDelimiter).append(this.sId)
                .append(FrameworkLevelConstant.KeyDelimiter).append(this.tm).toString();
    }

    public void assignAttribute(KeywordAttribute kwAttr) {
        if (kwAttr == null) {
            return;
        }
        // only assign and merge the attribute of the keywords

        // assign simple map
        for (Map.Entry<String, String> item : kwAttr.getSField().entrySet()) {
            this.sField.put(item.getKey(), item.getValue());
        }
        // assign map field
        for (Map.Entry<String, TreeMap<String, String>> item : kwAttr.getMField().entrySet()) {
            if (item.getValue() != null) {
                for (Map.Entry<String, String> kv : item.getValue().entrySet()) {
                    this.setMapFieldItem(item.getKey(), kv.getKey(), kv.getValue());
                }
            }
        }
        // assign set field
        for (Map.Entry<String, Set<String>> item : kwAttr.getUField().entrySet()) {
            if (item.getValue() != null) {
                for (String value : item.getValue()) {
                    this.addSetFieldItem(item.getKey(), value);
                }
            }
        }
        // assign nest field
        for (Map.Entry<String, TreeMap<String, TreeMap<String, String>>> firstLevelItem : kwAttr.getNField().entrySet()) {
            if (firstLevelItem.getValue() == null) {
                continue;
            }
            for (Map.Entry<String, TreeMap<String, String>> secLevelItem : firstLevelItem.getValue().entrySet()) {
                if (secLevelItem.getValue() == null) {
                    continue;
                }
                for (Map.Entry<String, String> kv : secLevelItem.getValue().entrySet()) {
                    this.setNestFieldItem(firstLevelItem.getKey(), secLevelItem.getKey(), kv.getKey(), kv.getValue());
                }
            }
        }
    }
}
