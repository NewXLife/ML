package sta;


import com.alibaba.fastjson.JSON;
import java.util.*;

public class Map2Json {
   public static void main(String args[]){
       Map<String, String> testmap = new HashMap<>();
       testmap.put("td_3month_platform_count","-Infinity,14.0,29.0,44.0,59.0,Infinity");
       testmap.put("td_6month_platform_count","-Infinity,15.8,32.6,49.400000000000006,66.2,Infinity");
       List<StatistiFeatureAndBinsPointVo> template = new ArrayList<>();
       StatistiFeatureAndBinsPointVo vo = new StatistiFeatureAndBinsPointVo();
       vo.setKeyFieldName("td_3month_platform_count");
       vo.setBin("-Infinity,14.0,29.0,44.0,59.0,Infinity");
       template.add(vo);

       StatistiFeatureAndBinsPointVo vo1 = new StatistiFeatureAndBinsPointVo();
       vo1.setKeyFieldName("td_6month_platform_count");
       vo1.setBin("-Infinity,14.0,29.0,44.0,59.0,Infinity");

       template.add(vo1);

       System.out.println(JSON.toJSONString(template));

       String str = "[{\"bin\":\"-Infinity,14.0,29.0,44.0,59.0,Infinity\",\"keyFieldName\":\"td_3month_platform_count\"},{\"bin\":\"-Infinity,14.0,29.0,44.0,59.0,Infinity\",\"keyFieldName\":\"td_6month_platform_count\"}]";
       List<StatistiFeatureAndBinsPointVo> res =  JSON.parseArray(str, StatistiFeatureAndBinsPointVo.class);

       for(StatistiFeatureAndBinsPointVo sv : res){
           List<String> binsList = Arrays.asList(sv.getBin().split(","));
           List arrList = new ArrayList();
           for(String s: binsList){
               arrList.add(Double.valueOf(s));
           }
           System.out.println(arrList);
           System.out.println(sv.getKeyFieldName());
       }

   }
}
