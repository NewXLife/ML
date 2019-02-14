package sta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrossAnalysisModel{

	/**特征名称**/
	private String featureName;	
	/**分箱模板分箱信息 Map<fieldName, "1.0, 2.0, 3.0, 4.0"> </fieldName,>*/
	private Map<String, List<String>>  binsTemplate = new HashMap<>();
	/**离线分箱时，超过阈值的，不进行分箱*/
    private Integer binningThreshold;
    
	public String getFeatureName() {
		return featureName;
	}
	
	public void setFeatureName(String featureName) {
		this.featureName = featureName;
	}

	public Map<String, List<String>> getBinsTemplate() {
		return binsTemplate;
	}

	public void setBinsTemplate(Map<String, List<String>> binsTemplate) {
		this.binsTemplate = binsTemplate;
	}

	public Integer getBinningThreshold() {
		return binningThreshold;
	}
	
	public void setBinningThreshold(Integer binningThreshold) {
		this.binningThreshold = binningThreshold;
	}

	@Override
	public String toString() {
		return "CrossAnalysisModel [featureName=" + featureName + ", binsTemplate=" + binsTemplate
				+ ", binningThreshold=" + binningThreshold + "]";
	}
    
}
