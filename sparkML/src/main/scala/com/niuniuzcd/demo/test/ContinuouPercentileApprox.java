package com.niuniuzcd.demo.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class ContinuouPercentileApprox extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = -6929025004186082706L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ContinuouPercentileApprox.class);

	@Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField( "valueField", DataTypes.DoubleType, true ));
        structFields.add(DataTypes.createStructField( "percentile", DataTypes.createArrayType(DataTypes.DoubleType), true ));
        return DataTypes.createStructType( structFields );
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField( "percentile", DataTypes.createArrayType(DataTypes.DoubleType), true ));	//近似分位点
        structFields.add(DataTypes.createStructField( "quantile", DataTypes.createArrayType(DataTypes.DoubleType), true ));		//业务分位点
        structFields.add(DataTypes.createStructField( "uppers", DataTypes.createArrayType(DataTypes.DoubleType), true ));		//上界异常点
        structFields.add(DataTypes.createStructField( "lowers", DataTypes.createArrayType(DataTypes.DoubleType), true ));		//下界异常点
        return DataTypes.createStructType(structFields );
    }

    @Override
    public DataType dataType() {
        return DataTypes.StringType;
    }

    @Override
    public boolean deterministic() {
    	//是否强制每次执行的结果相同
        return false;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
    	//初始化数据信息
        buffer.update(0, new ArrayList<Double>());		//近似分位点
        buffer.update(1, new ArrayList<Double>());		//业务分位点
        buffer.update(2, new ArrayList<Double>());		//上界异常点
        buffer.update(3, new ArrayList<Double>());		//下界异常点
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
    	//相同的executor间的数据合并
    	//近似分位点
    	Object tempPercentile = input.get(1);
    	if(tempPercentile != null) {
    		buffer.update(0, input.getList(1));
        	//计算分位点
        	List<Double> percentile = percentile(input.getList(1));
        	buffer.update(1, percentile);
        	//对值进行处理
        	Double value = input.getAs(0);
        	if(value != null && value > percentile.get(4)) {
        		//上界异常点
        		List<Double> temp = new ArrayList<>(buffer.getList(2));
        		temp.add(value);
        		buffer.update(2, temp);
        	}
        	else if(value != null && value < percentile.get(0)) {
        		//上界异常点
        		List<Double> temp = new ArrayList<>(buffer.getList(3));
        		temp.add(value);
        		buffer.update(3, temp);
        	}
    	}
    	else {
    		LOGGER.warn("Percentile is null");
    	}
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
    	//不同excutor间的数据合并
    	buffer1.update(0, buffer2.getList(0));
    	buffer1.update(1, buffer2.getList(1));

    	List<Double> temp1 = new ArrayList<>(buffer1.getList(2));
    	temp1.addAll(buffer2.getList(2));
    	buffer1.update(2, temp1);

    	List<Double> temp2 = new ArrayList<>(buffer1.getList(3));
    	temp2.addAll(buffer2.getList(3));
    	buffer1.update(3, temp2);
    }

    @Override
    public Object evaluate(Row buffer) {
    	//根据Buffer计算结果
    	Map<String, Object> map = new HashMap<>();
    	map.put("percentiles", buffer.getList(0));
    	map.put("quantiles",   buffer.getList(1));
    	//上界异常值
    	List<Double> upperValues = buffer.getList(2);
    	Map<Double, Long> distinctUpperValues = distinct(upperValues);
    	map.put("upperCount", upperValues.size());
    	map.put("upperDistinctCount", distinctUpperValues.size());
    	map.put("upperValues", distinctTop(distinctUpperValues, 200));
    	//下界异常值
    	List<Double> lowerValues = buffer.getList(3);
    	Map<Double, Long> distinctLowerValues = distinct(lowerValues);
    	map.put("lowerCount", lowerValues.size());
    	map.put("lowerDistinctCount", distinctLowerValues.size());
    	map.put("lowerValues", distinctTop(distinctLowerValues, 200));
    	//返回
    	try {
			return new ObjectMapper().writeValueAsString(map);
		} catch (Exception e) {
			return map.toString();
		}
    }

    /**
     * 	业务分位点计算指标
     * 	@param percentile
     * 	@return
     */
    private List<Double> percentile(List<Double> percentile){
    	Double upperNum = percentile.get(3) + (percentile.get(3) - percentile.get(1)) * 1.5;
    	Double lowerNum = percentile.get(1) - (percentile.get(3) - percentile.get(1)) * 1.5;
    	List<Double> temp = new ArrayList<>();
    	temp.add(lowerNum);
    	temp.add(percentile.get(1));
    	temp.add(percentile.get(2));
    	temp.add(percentile.get(3));
    	temp.add(upperNum);
    	return temp;
    }

    /**
     * 	去重
     * 	@param list
     * 	@return
     */
    private Map<Double, Long> distinct(List<Double> list){
    	//定义排重
    	Map<Double, Long> map = new TreeMap<>(new Comparator<Double>() {
			@Override
			public int compare(Double o1, Double o2) {
				return o1.compareTo(o2);
			}
		});
    	//排重
    	for(Double item : list) {
    		if(map.containsKey(item)) {
    			map.put(item, map.get(item) + 1);
    		}
    		else {
    			map.put(item, 1L);
    		}
    	}
    	//返回
    	return map;
    }

    /**
     * @param list
     * @param topN
     * @return
     */
    private static List<Map<String, Object>> distinctTop(Map<Double, Long> oriMap, int topN){
    	List<Map<String, Object>> res = new LinkedList<>();
    	if(oriMap != null && !oriMap.isEmpty()) {
    		//排序（oriMap）
    		List<Entry<Double, Long>> entryList = new ArrayList<Entry<Double, Long>>(oriMap.entrySet());
    		Collections.sort(entryList, new Comparator<Entry<Double, Long>>() {
				@Override
				public int compare(Entry<Double, Long> o1, Entry<Double, Long> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});
    		//排序（mapTopN）
    		Map<Double, Long> mapTopN = new TreeMap<>(new Comparator<Double>() {
    			@Override
    			public int compare(Double o1, Double o2) {
    				return o1.compareTo(o2);
    			}
    		});
    		//取值
    		int i = 0;
    		Iterator<Entry<Double, Long>> iter = entryList.iterator();
    		while (iter.hasNext()) {
    			Entry<Double, Long> tmpEntry = iter.next();
    			mapTopN.put(tmpEntry.getKey(), tmpEntry.getValue());
        		//取值topN
        		i = i + 1;
        		if(i >= topN) {
        			break;
        		}
    		}
    		//获取目标结构
    		for(Entry<Double, Long> value : mapTopN.entrySet()) {
        		Map<String, Object> temp = new LinkedHashMap<>();
        		temp.put("value", value.getKey());
        		temp.put("count", value.getValue());
        		res.add(temp);
        	}
    	}
		return res;
    }

}
