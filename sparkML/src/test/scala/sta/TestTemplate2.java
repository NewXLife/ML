package sta;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TestTemplate2 implements Serializable {
    private Map<String, List<String>> featureBins;

    public Map<String, List<String>> getFeatureBins() {
        return featureBins;
    }

    public void setFeatureBins(Map<String, List<String>> featureBins) {
        this.featureBins = featureBins;
    }
}
