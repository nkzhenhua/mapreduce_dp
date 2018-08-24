package dataplatform.framework.keyword;

public enum DataSourceType {
    String sourceType;

    DataSourceType(String type) {
        this.sourceType = type;
    }

    public String getSourceType() {
        return this.sourceType;
    }
}
