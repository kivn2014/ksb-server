package com.ksb.openapi.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.util.SystemConst;

import redis.clients.jedis.GeoParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Service
public class WaybillGEOService {

	private Logger log = LogManager.getLogger(getClass());
	
	private static long intervalDistance = 5;
	
	private static int groupMaxNum = 3;
	
	private static int searchBaseRadius = 3;
	
	//@Autowired
	private static JedisPool jedisPool = null;
	
	static{
		
        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxActive(500);
        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(5);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
//        config.setMaxWait(1000 * 100);
        //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, "101.200.228.188", 6379);
		
		
//		try{
//			/*设置优化结果中，每个配送地址相距最大距离*/
//		    String tempDistance = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_GROUP_DISTANCE);
//		    intervalDistance = Long.parseLong(tempDistance);
//		
//		    /*一个优化分组单元，每个分组中最多元素个数*/
//		    String tempgroupMaxNum = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_GROUP_DISTANCE);
//		    groupMaxNum = Integer.parseInt(tempgroupMaxNum);
//		    
//		    /*搜索半径*/
//		    String tempBaseRadius = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_BASE_RADIUS);
//		    searchBaseRadius = Integer.parseInt(tempBaseRadius);		    
//		
//		}catch(Exception e){}
	}
	
	
	public static void main(String[] args) throws IOException {
		
		File file = new File("/Users/houshipeng/poi_data/zhongguancun.csv");
		List<String> list = FileUtils.readLines(file, "utf-8");
		
		List<String> waybillList = new ArrayList<String>();
		for(String l : list){
			waybillList.add(l.split(",")[0].replace("'", ""));
		}
		WaybillGEOService s = new WaybillGEOService();
		List<List<String>> rsList = s.optimizeGroupWaybill(116.31905437572878, 39.983575110281514, "kevi", waybillList);
		System.out.println(rsList);
		
	}
	
	
	public List<List<String>> optimizeGroupWaybill(double x,double y,String batchId,List<String> waybillList){
		
		List<List<String>> returnList = new ArrayList<List<String>>();
		List<List<String>> threeList = new ArrayList<List<String>>();
		List<List<String>> twoList = new ArrayList<List<String>>();
		List<String> oneList = new ArrayList<String>();
		
		while(waybillList.size()>0){
			List<String> tmpList = handleWaybill(x, y, batchId, searchBaseRadius, waybillList);
			System.out.println(tmpList.size());
			int sk = tmpList.size();
			if(sk==0){
				/*无法搜索到数据*/
				break;
			}
			
			switch (sk) {
			case 1:
				oneList.add(tmpList.get(0));
				break;
			case 2:
				twoList.add(tmpList);
				break;
			case 3:
				threeList.add(tmpList);
				break;
			default:
				break;
			}
			
		}
		
		returnList.addAll(twoList);
		returnList.addAll(threeList);
		
		/*没有孤点*/
		if(oneList.size()==0){
			return returnList;
		}
		
		/*处理孤点*/
		
		/*如果第一次处理后，还有运单未被处理，则把剩余的运单放到第二次待处理列表中(防止有运单漏处理)*/
		//oneList.addAll(waybillList);
		
		/*所有的孤点放到redis中，重新计算*/
		Map<String, String> geoMap = new HashMap<String, String>();
		List<Double> distanceList = new ArrayList<Double>();
		
		/*把所有的孤点聚合起来，重新执行一遍*/
		List<String> tmpOneList = new ArrayList<String>();
		
		for(String str : oneList){
			String ss[] = str.split(",");
			geoMap.put(ss[0], ss[1]+";"+ss[2]);
			distanceList.add(Double.parseDouble(ss[3]));
			tmpOneList.add(ss[0]);
		}
		oneShipperMoreBuyerGEO2Redis(batchId, geoMap);
		
		/*重新计算半径(新半径=(max+min)/2)*/
		Double newRadius = (Collections.max(distanceList)+Collections.min(distanceList))/2;
		
		
		List<String> lastOneList = new ArrayList<String>();
		while(tmpOneList.size()>0){
			List<String> tl =  handleWaybill(x, y, batchId, newRadius.intValue()+1, tmpOneList);
			if(tl==null||tl.size()==0){
				break;
			}
			if(tl.size()>1){
				returnList.add(tl);
			}else if(tl.size()==1){
				lastOneList.add(tl.get(0));
			}
		}
		
		/*如果还有孤点，将不再继续处理，按顺序每三个合成一组*/
		//lastOneList.addAll(oneList);
		
		int fromIndex = 0;
		int toIndex = fromIndex + 3;
		if(toIndex>=lastOneList.size()){
			toIndex = lastOneList.size();
		}
		while(true){
			returnList.add(lastOneList.subList(fromIndex, toIndex));
			fromIndex += 3;
			toIndex = fromIndex+3;

			if(fromIndex>=lastOneList.size()){
				break;
			}
			
			if(toIndex>=lastOneList.size()){
				toIndex = lastOneList.size();
			}
		}
		
		return returnList;
	}
	
	
	
	/**
	 * 临时算法，以后会根据实际情况，逐步优化
	 * @param x
	 * @param y
	 * @param batchId
	 * @param waybillList
	 */
	private List<String> handleWaybill(double x,double y,String batchId,int radius,List<String> waybillList){
	
		/*当前中心点*/
		double currentRadiusX = x;
		double currentRadiusY = y;
		
		int maxNum = groupMaxNum;
//		int radius = searchBaseRadius;
		
		
		List<String> rsList = new ArrayList<String>();
		ResultEntity rs = groupWaybill(x, y, batchId, radius,false);
		
		if(rs.isSuccess()){
			
			/*搜索到第一个附近点*/
			String geoInfo = rs.getObj().toString();
			String[] geos = geoInfo.split(",");
			String id = geos[0];
			
			rsList.add(geoInfo);
			/*从redis中删除已经处理的数据，防止下次从中心点查询的时候再次出现*/
			this.removeGeoNumInRedis(batchId, id);
			waybillList.remove(id);
			
			/*修改中心点为当前查询到得最近点*/
			currentRadiusX = Double.parseDouble(geos[1]);
			currentRadiusY = Double.parseDouble(geos[2]);
			
			/*以当前点为中心点，搜索下一个最近的点*/
			rs = groupWaybill(currentRadiusX, currentRadiusY, batchId, radius,true);
			if(rs.isSuccess()){
				/*搜索到第二个附近点*/
				geoInfo = rs.getObj().toString();
				geos = geoInfo.split(",");
				id = geos[0];
				rsList.add(geoInfo);
				/*从redis中删除已经处理的数据，防止下次从中心点查询的时候再次出现*/
				this.removeGeoNumInRedis(batchId, id);
				waybillList.remove(id);
				
				/*修改中心点为当前查询到得最近点*/
				currentRadiusX = Double.parseDouble(geos[1]);
				currentRadiusY = Double.parseDouble(geos[2]);
				
				/*执行第三次搜索*/
				rs = groupWaybill(x, y, batchId, radius,true);
				if(rs.isSuccess()){
					/*搜索到第三个附近点*/
					geoInfo = rs.getObj().toString();
					geos = geoInfo.split(",");
					id = geos[0];
					rsList.add(geoInfo);
					/*从redis中删除已经处理的数据，防止下次从中心点查询的时候再次出现*/
					this.removeGeoNumInRedis(batchId, id);
					waybillList.remove(id);
					
					return rsList;
				}else{
					/*直接返回*/
					return rsList;
				}
				
			}else{
				/*直接返回*/
				return rsList;
			}
			
		}else{
			/*没找到符合条件的点（需要根据返回的错误信息 判断是否需要继续搜索）*/
			String rsCode = rs.getErrors();
			
			if(rsCode.equals("no")){
				/*只有根据原始中心点，查询第一个点  无结果的时候，半径才翻倍*/
				return handleWaybill(currentRadiusX, currentRadiusY, batchId, radius*2, waybillList);
			}else if(rsCode.equals("reserve")){
				/*两点之间的距离超过指定的范围*/
				//waybillList.remove(o)
			}else if(rsCode.equals("max")){
				/*搜索半径太大*/
				
			}else if(rsCode.equals("error")){
				/*异常*/
				
			}
		}
		
		return rsList;
	}
	
	/**
	 * 从redis中删除一个已经合并的数据
	 * @param batchId
	 * @param waybillId
	 */
	private void removeGeoNumInRedis(String batchId,String waybillId){
		
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
//			String prefixKey = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_ONE2MORE_GEO_KEY);
			String prefixKey = "redis_key";
			
			jedis.zrem(prefixKey+"_"+batchId, waybillId);
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(jedis!=null){
				jedis.close();
			}
		}
		
	}
	
	
	/**
	 * 运单优化工具类，改方法仅获取离中心点最近的一个点
	 * @param x 中心点经度
	 * @param y 中心点纬度
	 * @param batchId     要处理的批次号
	 * @param radius      数据查找半径                   
	 */
	private ResultEntity groupWaybill(double x,double y,String batchId,int radius,boolean isValidate){
		
		ResultEntity rsEntity = new ResultEntity();
		
		/*防止搜索半径过大*/
		if(radius>500){
			//return resultList;
			rsEntity.setSuccess(false);
			rsEntity.setErrors("max");
			return rsEntity;
		}
		
		Jedis jedis = null;
		
		/*从redis中取出要优化分组的运单数据*/
		try {
//			String prefixKey = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_ONE2MORE_GEO_KEY);
			String prefixKey = "redis_key";
			
			
			jedis = jedisPool.getResource();
			
			/*默认查找3公里范围内的数据，如果数据量少或者没有数据，当前范围值*2 */
			String redisKey = prefixKey+"_"+batchId;
			
			GeoParams geoParam = new GeoParams();
			/*设置查找半径*/
			geoParam.radius(String.valueOf(radius));
			/*设置查询结果格式(显示距中心点的距离)*/
			//geoParam.geodistance();
			geoParam.geojson();
			/*查找的结果按距离从近到远排序*/
			geoParam.asc();
			
			List<String> rsList = jedis.geoscanByRadius(redisKey, y, x, geoParam);
			if(rsList==null||rsList.size()==0){
				rsEntity.setSuccess(false);
				rsEntity.setErrors("no");
				return rsEntity;
				/*如果没有结果 当前半径*2 再次查询一次*/
//				groupWaybill(x, y, batchId, radius*2);
			}
			
			/*结果是按距离排序的，取结果集第一个即可*/
			String rs = rsList.get(0);
			String[] infos = rs.split("\\^");

			/* 运单ID */
			String waybillId = infos[0];

			/* 该运单空间数据(包含经纬度、距中心点距离) */
			String geoJson = infos[1];

			/* 解析json格式的结果数据，从中提前经纬度和距中心点距离数据 */
			JSONObject jsonObj = JSON.parseObject(geoJson);
			JSONArray coorsArray = jsonObj.getJSONObject("geometry").getJSONArray("coordinates");

			/* 距中心点最近的点 */
			String tmpX = coorsArray.get(0).toString();
			String tmpY = coorsArray.get(1).toString();

			/*获取距中心点的距离(如果最近的点都比较远的话，需要考虑其他的优化或者解决方案)*/	
			double distance = jsonObj.getJSONObject("properties").getDouble("distance");	
			
			/*第一次查找(从中心发起的查询 不验证两点之间的距离)*/
			if (isValidate) {
				if (distance > intervalDistance) {
					/* 不合单或者考虑其他方案 */
					rsEntity.setSuccess(false);
					rsEntity.setErrors("reserve");
					return rsEntity;
				}
			}	
			rsEntity.setSuccess(true);
			rsEntity.setErrors("ok");
			rsEntity.setObj(waybillId+","+tmpX+","+tmpY+","+distance);
			return rsEntity;
			
		} catch (Exception e) {
			log.error("导入批次["+batchId+"] 本地优化分组失败");
		}finally{
			if(jedis!=null){
				jedis.close();
			}
		}
		rsEntity.setSuccess(false);
		rsEntity.setErrors("error");
		return rsEntity;
	}
	
	/**
	 * 处理通过openapi批量导入数据的用户（主要解决一个客户 一次性发多个运单的场景）
	 * @param batchId
	 * @param geoMap
	 */
	public void oneShipperMoreBuyerGEO2Redis(String batchId,Map<String, String> geoMap){
		Jedis jedis = null;
		try {
//			String key = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.KSB_WAYBILL_ONE2MORE_GEO_KEY);
			String key = "redis_key";
			
			
			jedis = jedisPool.getResource();
			if(geoMap==null || geoMap.size()==0){
				return;
			}
			
			/*防止重复，数据入redis之前先执行删除(batchId 不会出现重复，所以不会影响其他线程的数据)*/
			jedis.del(key+"_"+batchId);
			
			Iterator<Entry<String, String>> it = geoMap.entrySet().iterator();
			while(it.hasNext()){
				Entry<String, String> entry = it.next();
				String waybillId = entry.getKey();
				String buyerXY = entry.getValue();
				String xys[] = buyerXY.split(";");
				double x = Double.parseDouble(xys[0]);
				double y = Double.parseDouble(xys[1]);
				jedis.geoset(key+"_"+batchId, y, x, waybillId);
			}
		} catch (Exception e) {
			log.error("缓存数据失败", e);
		} finally {
			if (jedis != null){
				jedis.close();;
			}
		}
	}
	
}
