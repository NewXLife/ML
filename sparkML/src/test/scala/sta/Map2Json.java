package sta;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class Map2Json {
    enum Test{
        continuous,
        categories
    }

    public static Map<String, String> getMapString(){
        Map<String,String>  res = new HashMap<>();
        res.put("m18","(2,3],(3,4]");
        res.put("m60","(小学),(初中),(大学;博士)");
        return res;
    }

    public static List<CrossAnalysisModel> getCrossList(){
        List<CrossAnalysisModel> resList = new ArrayList<>();

        CrossAnalysisModel cm  = new CrossAnalysisModel();
        cm.setBinningThreshold(6);
        cm.setFeatureName("m60");
        cm.setBinsTemplate(getJavaMap());
        CrossAnalysisModel cm1  = new CrossAnalysisModel();
       cm1.setBinningThreshold(5);
       cm1.setFeatureName("m1");
       cm1.setBinsTemplate(getJavaMap2());

        resList.add(cm);
        resList.add(cm1);
        return  resList;
    }

    public static  Map<String,List<String>> getJavaMap2(){
        Map<String,List<String>>  res = new HashMap<>();
        List<String>  list2 = new ArrayList<>();
        list2.add("(2,5]");
        list2.add("(5,11]");
        res.put("m1", list2);
        return res;
    }

    public static  Map<String,List<String>> getJavaMap(){
            Map<String,List<String>>  res = new HashMap<>();
            List<String>  list1 = new ArrayList<>();
            list1.add("[\"小学\"");
            list1.add("" +
                    "\"初中\"]");
            list1.add("大学,博士");

        List<String>  list2 = new ArrayList<>();
        list2.add("(2,3]");
        list2.add("(3,4]");

//        res.put("m18", list2);
        res.put("m60", list1);
        return res;
    }

    public static  Map<String,List<String>> getJavaMap1(){
        Map<String,List<String>>  res = new HashMap<>();
        List<String>  list1 = new ArrayList<>();
        list1.add("大学");
        list1.add("小学");
        list1.add("（初中，大学）");
        list1.add("初中）");
        res.put("f1", list1);
        return res;
    }

    public static void main(String args[]) {
        System.out.println(Test.categories.name());
        String str = "{\"categories\":[\"f2\"],\"f1\":[\"-Infinity\",\"1\",\"2\",\"3\",\"Infinity\"],\"f2\":[\"初中\",\"初(中\",\"大学\",\"大学,初中\"],\"continuous\":[\"f1\"]}";
//    System.out.println(str);
    //{"categories":["f2"],"f1":["-Infinity","1","2","3","Infinity"],"f2":["初中","初(中","大学","大学,初中"],"continuous":["f1"]}
        Map<String,List<String>> res = JSONObject.parseObject(str, new TypeReference<Map<String,List<String>>>(){});
//        System.out.println(res);
//        System.out.println(JSONObject.toJSONString(res));
//        for (String key : res.keySet()) {
//            if(key != null && !key.equals("categories") && !key.equals("continuous"))
//                System.out.println(StringUtils.join(res.get(key), "&"));
//                res.put(key,StringUtils.join(res.get(key), ","));
        }
//    }
}

//       Map<String, String> testmap = new HashMap<>();
//       testmap.put("td_3month_platform_count","-Infinity,14.0,29.0,44.0,59.0,Infinity");
//       testmap.put("td_6month_platform_count","-Infinity,15.8,32.6,49.400000000000006,66.2,Infinity");
//       List<StatistiFeatureAndBinsPointVo> template = new ArrayList<>();
//       StatistiFeatureAndBinsPointVo vo = new StatistiFeatureAndBinsPointVo();
//       vo.setKeyFieldName("td_3month_platform_count");
//       vo.setBin("-Infinity,14.0,29.0,44.0,59.0,Infinity");
//       template.add(vo);
//
//       StatistiFeatureAndBinsPointVo vo1 = new StatistiFeatureAndBinsPointVo();
//       vo1.setKeyFieldName("td_6month_platform_count");
//       vo1.setBin("-Infinity,14.0,29.0,44.0,59.0,Infinity");
//
//       template.add(vo1);
//
//       System.out.println(JSON.toJSONString(template));
//
//       String str = "[{\"bin\":\"-Infinity,14.0,29.0,44.0,59.0,Infinity\",\"keyFieldName\":\"td_3month_platform_count\"},{\"bin\":\"-Infinity,14.0,29.0,44.0,59.0,Infinity\",\"keyFieldName\":\"td_6month_platform_count\"}]";
//       List<StatistiFeatureAndBinsPointVo> res =  JSON.parseArray(str, StatistiFeatureAndBinsPointVo.class);
//
//       for(StatistiFeatureAndBinsPointVo sv : res){
//           List<String> binsList = Arrays.asList(sv.getBin().split(","));
//           List arrList = new ArrayList();
//           for(String s: binsList){
//               arrList.add(Double.valueOf(s));
//           }
//           System.out.println(arrList);
//           System.out.println(sv.getKeyFieldName());
//       }
