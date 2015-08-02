package com.ksb.openapi.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.sharding.dao.DaoHelper;

import com.ksb.openapi.dao.StatisticsDao;
import com.ksb.openapi.entity.PairStatEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.service.StatisticsService;
import com.ksb.openapi.util.DateUtil;

@Service
public class StatisticsServiceImpl implements StatisticsService {

	private Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	StatisticsDao statisticsDao;
	
	@Override
	public Map<String, Object> groupQueryDateStatus(String enterpriseId,Long startTime, Long endTime,int skip,int size) {
		// TODO Auto-generated method stub
		log.entry(enterpriseId,startTime,endTime,skip,size);
		if(StringUtils.isBlank(enterpriseId)){
			throw new BaseSupportException("配送公司企业编号为空");
		}
		
		if(startTime==null || endTime==null){
			throw new BaseSupportException("请指定要统计的时间段");
		}
		
		List<Map<String, String>> rsList = null;
		List<Map<String, String>> countList = null;
		long countNum = 0;
		try{
			
			/*查询时间段内的总量*/
			countList = (List<Map<String, String>>)statisticsDao.groupQueryCountDateStatus(enterpriseId, startTime, endTime);
			log.entry("statisticsDao.groupQueryCountDateStatus",rsList);
			
			/*时间段内分页查询数据*/
			rsList = (List<Map<String, String>>)statisticsDao.groupQueryDateStatus(enterpriseId, startTime, endTime,skip,size);
			log.entry("statisticsDao.groupQueryDateStatus_return",rsList);
			countNum = DaoHelper.getCount();
			log.entry("statisticsDao.groupQueryDateStatus_return_count",countNum);
			
			
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Map<String, String>> tmMap = new TreeMap<String, Map<String,String>>();
		Map<String, String> tmCountMap = new HashMap<String,String>();
		Map<String, Object> rsMap = new HashMap<String, Object>();
		
		/*封装从数据库中返回的数据，按日期合并订单的不同状态数据*/
		handleDateStatusList(rsList, tmMap);
		handleCountData(countList, tmCountMap);
		
		rsMap.put("1", tmMap);
		rsMap.put("2", countNum);
		rsMap.put("3", tmCountMap);
		
		return rsMap;
	}

	private void handleCountData(List<Map<String, String>> rsList,Map<String, String> rsMap){
		
		if(rsList==null || rsList.size()==0){
			return;
		}
		
		for(Map<String, String> map : rsList){
			String status = String.valueOf(map.get("waybill_status"));
			String statusNum = String.valueOf(map.get("status_num"));
			rsMap.put(String.valueOf(status), String.valueOf(statusNum));
		}
		
	}
	
	/**
	 * 日期和状态分组统计封装
	 * @param rsList
	 * @param rsMap
	 * @return
	 */
	private void handleDateStatusList(List<Map<String, String>> rsList,Map<String, Map<String, String>> rsMap){
		
		if(rsList==null || rsList.size()==0){
			return;
		}
		
		for(Map<String, String> map : rsList){
			
			String wbDate = map.get("waybill_date");
			String status = String.valueOf(map.get("waybill_status"));
			String statusNum = String.valueOf(map.get("status_num"));
			
			if(rsMap.containsKey(wbDate)){
				rsMap.get(wbDate).put(String.valueOf(status), String.valueOf(statusNum));
			}else{
				Map<String, String> tm = getDefaultValue();
				tm.put(String.valueOf(status), String.valueOf(statusNum));
				
				rsMap.put(wbDate, tm);
			}
		}
	}
	
	private Map<String, String> getDefaultValue(){
		
		Map<String, String> tm = new HashMap<String, String>();
		tm.put("-3", "0");
		tm.put("5", "0");
		return tm;
	}
	
	
	
	@Override
	public Map<String, Object> groupQueryCourierStatusByDate(String enterpriseId,String courierRealName,String dayStr,int skip,int size){
		// TODO Auto-generated method stub
		
		if(StringUtils.isBlank(enterpriseId)){
			throw new BaseSupportException("配送公司企业编号为空");
		}
		
		if(StringUtils.isBlank(dayStr)){
			throw new BaseSupportException("请指定要统计的时间");
		}
		
		/*真实姓名支持like查询*/
		if(StringUtils.isNotBlank(courierRealName)){
			courierRealName = "%"+courierRealName+"%";
		}
		
		
		
		List<Map<String, String>> rsList = null;
		long countNum = 0;
		try{
			rsList = (List<Map<String, String>>)statisticsDao.queryCourierStatusByDate(enterpriseId, courierRealName, dayStr,skip,size);
			countNum = DaoHelper.getCount();
			
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		Map<String, Map<String, String>> tmMap = new TreeMap<String, Map<String, String>>();
		
		handleCourierStatusList(rsList, tmMap);
		rsMap.put("1", tmMap);
		rsMap.put("2", countNum);
		
		return rsMap;
	}
	
	
	private void handleCourierStatusList(List<Map<String, String>> rsList,Map<String, Map<String, String>> rsMap){
		
		if(rsList==null || rsList.size()==0){
			return;
		}
		
		for(Map<String, String> map : rsList){
			
			String realName = map.get("courier_real_name");
			String status = String.valueOf(map.get("waybill_status"));
			String statusNum = String.valueOf(map.get("status_num"));
			
			if(rsMap.containsKey(realName)){
				rsMap.get(realName).put(String.valueOf(status), String.valueOf(statusNum));
			}else{
				Map<String, String> tm = new HashMap<String, String>();
				tm.put(String.valueOf(status), String.valueOf(statusNum));
				
				rsMap.put(realName, tm);
			}
		}
	}

	@Override
	public Map<String, Map<String, String>> groupQueryShipperStatusByDate(String shipperId,
			String shipperUserId, Long startTime, Long endTime) {
		// TODO Auto-generated method stub
		
		if(StringUtils.isBlank(shipperId)){
			throw new BaseSupportException("商家编号为空");
		}
		
		if(startTime==null || endTime==null){
			throw new BaseSupportException("请指定查询时间范围");
		}
		
		List<Map<String, String>> rsList = null;
		try{
			rsList = (List<Map<String,String>>)statisticsDao.queryShipperStatusByDate(shipperId, shipperUserId, startTime, endTime);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Map<String, String>> rsMap = new TreeMap<String, Map<String,String>>();
		handleDateStatusList(rsList, rsMap);
		
		return rsMap;
	}
	

}
