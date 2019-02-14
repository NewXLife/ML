package sta;

import java.io.Serializable;

public class StatistiFeatureAndBinsPointVo implements Serializable {
    /**
     *	字段名称
     */
    private String keyFieldName;

    /**
     *	分箱
     */
    private String bin;

    public String getKeyFieldName() {
        return keyFieldName;
    }

    public void setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }

    public String getBin() {
        return bin;
    }

    public void setBin(String bin) {
        this.bin = bin;
    }
}
