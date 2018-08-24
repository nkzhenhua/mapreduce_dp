package dataplatform.framework.keyword;

public enum DataSourceId {
	KWR(0);
	MUST_HAVE(15);
    int id;

    DataSourceId(int id) {
        this.id = id;
    }

    public int getSourceId() {
        return this.id;
    }
}
