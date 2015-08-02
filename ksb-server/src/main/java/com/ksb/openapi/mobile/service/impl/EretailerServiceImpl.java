package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import redis.clients.jedis.JedisPool;
import autonavi.online.framework.property.PropertiesConfig;
import autonavi.online.framework.property.PropertiesConfigUtil;
import autonavi.online.framework.sharding.dao.DaoHelper;
import autonavi.online.framework.zookeeper.SysProps;

import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.em.WaybillType;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.EretailerService;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.JedisUtil;
import com.ksb.openapi.util.SystemConst;



@Service
public class EretailerServiceImpl implements EretailerService {

	@Autowired
	WaybillDao waybillDao = null;
	
	@Autowired
	UserDao userDao = null;
	
	private String WAYBILL_TYPE = WaybillType.OL_RETAILERS.getName();
	
	private String REDIS_KEY_PREFIX = "OL";
	
	
	
	private Logger log = LogManager.getLogger(getClass());
	
	@Override
	public WayBillEntity queryOlWaybill(String shipperOrderId) {
		// TODO Auto-generated method stub
		log.entry(shipperOrderId);
		return log.exit((WayBillEntity)waybillDao.queryOlWaybillById(shipperOrderId,null));
	}

	@Override
	public WayBillEntity queryOlWayBillByKsbId(String waybillId) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 电商运单入库
	 */
	@Override
	public void createOlWaybill(Map<String, String> paraMap) {
		// TODO Auto-generated method stub

		log.entry(paraMap);
		if(paraMap==null||paraMap.size()==0){
			log.error("paraMap为空,未提供任何参数");
			throw new BaseSupportException("未提供任何参数");
		}
		
		/*前期开发，需要知道存储那些字段，业务成熟以后以后优化，通过反射把map对象转换为Waybillentity对象*/
		
		/*快递员ID，当前的操作者*/
		String courierId = paraMap.get("cid");
        if(StringUtils.isBlank(courierId)){
        	log.error("电商的运单ID字段为空");
        	throw new BaseSupportException("电商的运单ID字段为空");
        }
        
		String olOrderId = paraMap.get("oid");
        if(StringUtils.isBlank(olOrderId)){
        	log.error("电商的运单ID字段为空");
        	throw new BaseSupportException("电商的运单ID字段为空");
        }
        
        String isTopay = paraMap.get("is_topay");
        if(StringUtils.isBlank(isTopay)){
        	log.error("运单是否到付字段为空");
        	throw new BaseSupportException("运单是否到付字段为空");
        }
        
        /*如果选择到付，必须输入到付的金额*/
        String topayFee = paraMap.get("fee");
        if(isTopay.equals("1")){
        	if(StringUtils.isBlank(topayFee)){
        		log.error("到付金额为空");
        		throw new BaseSupportException("到付金额为空");
        	}
        }
        if(StringUtils.isBlank(topayFee)){
        	paraMap.put("fee", "0");
        }
        String remarks = paraMap.get("remarks");
        if(StringUtils.isBlank(remarks)){
        	paraMap.put("remarks", "");
        }
        
        /*在此判断该运单是否已经存在(存在未配送的运单，防止重复提交)*/
        
       WayBillEntity searchEntity = (WayBillEntity)waybillDao.queryOlWaybillById(olOrderId, null);
       if(searchEntity!=null){
    	  String searchStatus = searchEntity.getWaybill_status();
    	  if(searchStatus.equals("1")||searchStatus.equals("0")){
    		  log.error(olOrderId+"在配送列表中，不能重复提交");
    		  throw new BaseSupportException("不能重复提交");
    	  }
       }
        
//        /*电商ID*/
        String spId = paraMap.get("sp_id");
        if(StringUtils.isBlank(spId)){
        	throw new BaseSupportException("未指定电商");
        }
        
        WayBillEntity waybillEntity = new WayBillEntity();
        
        /*电商运单ID*/
        waybillEntity.setShipper_origin_id(olOrderId);
        /*运单隶属的电商*/
        waybillEntity.setShippers_id(spId);
        /*到付应收金额*/
        waybillEntity.setCargo_price(topayFee);
        /*是否需要到付*/
        waybillEntity.setIs_topay(isTopay);
        /*备注*/
        waybillEntity.setRemarks(remarks);
        /*使用快送宝配送平台*/
        waybillEntity.setThird_platform_id("0");
        /*运单状态(电商运单只有三个状态：配送中、完成、异常件)*/
        waybillEntity.setWaybill_status("1");
        waybillEntity.setPayment_status("0");
        waybillEntity.setWaybill_type(WAYBILL_TYPE);
        try {
			waybillEntity.setId("3km-"+DaoHelper.createPrimaryKey().toString());
		} catch (Exception e1) {throw new BaseSupportException(e1);}
        /*快递员ID(当天操作者)*/
        waybillEntity.setCourier_id(courierId);
        
        List<WayBillEntity> list = new ArrayList<WayBillEntity>();
        list.add(waybillEntity);
		
        try{
        	
		    waybillDao.batchCreateKSBWayBill(list);
        }catch(Exception e){
        	log.error(e.getMessage());
        	throw new BaseSupportException(e);
        }
        
		boolean redisEnable = false;
		try {
			/* 判断是否启用redis */
			String redisEnableStr = (String) PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.REDIS_ENABLE);
			log.debug("是否启用redis: " + redisEnableStr);
			redisEnable = Boolean.parseBoolean(redisEnableStr);
			
			if(redisEnable){
				/* redis缓存中初始化 运单状态 */
				paraMap.put("waybill_status", "1");
				paraMap.put("waybill_id", waybillEntity.getId());

				/* 同步把快递员当天的 运单状态统计数据写入到redis中 */
				/* key格式 */
				String key = REDIS_KEY_PREFIX
						+ "_"
						+ courierId
						+ "_1_"
						+ DateUtil.convertDateToString(DateUtil.getCurrentDate(),"yyyyMMdd");
				try {
					JedisUtil.getInstance().SETS.sadd(key, olOrderId);
				} catch (Exception e) {}

				/* 异步把刚入库的数据写到redis中 */
				OlWaybill2Redis waybill2Redis = new OlWaybill2Redis(paraMap);
				waybill2Redis.run();
			}
		} catch (Exception e) {
		}
	}

	/**
	 * 扫描的运单入redis库
	 * @author houshipeng
	 *
	 */
	public class OlWaybill2Redis extends Thread{
		
		Map<String, String> map = null;
		public OlWaybill2Redis(Map<String, String> map){
			this.map = map;
		}
		
		@Override
		public void run() {
			
			/*redis中 key的 格式： 快递员ID_电商运单ID_年月日  */
			String courierId = map.get("cid");
			String orderId = map.get("oid");
			String todayStr = DateUtil.convertDateToString(DateUtil.getCurrentDate(),"yyyyMMdd");
			
			String redisKey = REDIS_KEY_PREFIX+"_"+courierId+"_"+orderId+"_"+todayStr;
			
			boolean redisEnable = false;
			try{
			    String redisEnableStr = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.REDIS_ENABLE);
			    redisEnable = Boolean.parseBoolean(redisEnableStr);
			    if(redisEnable){
				    JedisUtil.getInstance().HASH.hmset(redisKey, map);
				    /*有效期2天*/
				    JedisUtil.getInstance().expire(redisKey, 48*3600);
			    }
			}catch(Exception e){}
		}
	}

	/**
	 * 扫描电商运单
	 * 扫描的结果是 返回的对象有值，则说明订单已经存在，不能再次入库，但是可以执行配送完成操作
	 *            返回的对象为空，则说明该电商的订单不存在，可以直接入库
	 *            方法抛异常，说明该订单已经配送完成，不能重复操作
	 */
	@Override
	public WayBillEntity scanOlWaybill(String courierId,String olOrderId){
		log.entry(courierId,olOrderId);

		Map<String, String> olwaybillMap = null;
		
		boolean redisEnable = false;
		try{
		    String redisEnableStr = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.REDIS_ENABLE);
		    redisEnable = Boolean.parseBoolean(redisEnableStr);
		    if(redisEnable){
				/*先从redis中查找是否有该运单*/
				String redisKey = REDIS_KEY_PREFIX+"_"+courierId+"_"+olOrderId+"_"+DateUtil.convertDateToString(DateUtil.getCurrentDate(),"yyyyMMdd");
				
		    	olwaybillMap = JedisUtil.getInstance().HASH.hgetAll(redisKey);
		    }
		}catch(Exception e){}
		
		if(olwaybillMap==null||olwaybillMap.size()==0){
			
			/*说明缓存中没有,防止数据库和缓存不同步，从数据库中再次读取一遍*/
			WayBillEntity waybillEntity = (WayBillEntity)waybillDao.queryOlWaybillById(olOrderId,null);
			if(waybillEntity ==null){
				/*说明数据库中也不存在该记录*/
				return null;
			}
			
			String currentStatus = waybillEntity.getWaybill_status();
			
			/*缓存和数据库不同步，需要数据同步*/
			if(currentStatus.equals("1")){
				Map<String, String> paraMap = new HashMap<String, String>();
				paraMap.put("oid", olOrderId);
				paraMap.put("is_topay", waybillEntity.getIs_topay());
				paraMap.put("fee", waybillEntity.getCargo_price());
				paraMap.put("remarks", waybillEntity.getRemarks()==null?"":waybillEntity.getRemarks());
				paraMap.put("status", "1");
				paraMap.put("cid", waybillEntity.getCourier_id());
				paraMap.put("id", waybillEntity.getId());
				paraMap.put("shipper_origin_id", waybillEntity.getShipper_origin_id());
				
				/*重新赋值*/
				olwaybillMap = new HashMap<String, String>();
				olwaybillMap.putAll(paraMap);
				
				if(redisEnable){
			        /*异步把刚入库的数据写到redis中*/
			        OlWaybill2Redis waybill2Redis = new OlWaybill2Redis(paraMap);
			        waybill2Redis.run();
				}
			}else if(currentStatus.equals("5")){
				throw new BaseSupportException("不能执行配送完成的运单");
			}else if(currentStatus.equals("-1")){
				return null;
			}
		}
		
		
		/*运单不存在 扫描动作是入库(两种情况可以入库：1、未处理过的运单；2、异常件重新处理)*/
		if(olwaybillMap==null||olwaybillMap.size()==0){
			return null;
		}
		
		String statusStr = olwaybillMap.get("status");
		int statusInt = Integer.parseInt(statusStr);
		
		/*状态小于0，说明是异常件*/
		if(statusInt<0){
			return null;
		}
		
		/*状态为5，表示已经完成配送，不能再扫描执行其他动作*/
		if(statusInt==5){
			throw new BaseSupportException("不能执行配送完成的运单");
		}
		
        WayBillEntity waybillEntity = new WayBillEntity();
        
        /*电商运单ID*/
        waybillEntity.setShipper_origin_id(olOrderId);
        /*到付应收金额*/
        waybillEntity.setCargo_price(olwaybillMap.get("fee"));
        /*是否需要到付*/
        waybillEntity.setIs_topay(olwaybillMap.get("is_topay"));
        /*备注*/
        waybillEntity.setRemarks(olwaybillMap.get("remarks"));
        /*状态*/
        waybillEntity.setWaybill_status(statusStr);
        waybillEntity.setId(olwaybillMap.get("id"));
        waybillEntity.setShipper_origin_id(olwaybillMap.get("shipper_origin_id"));
		return waybillEntity;
	}
	
	@Override
	public List<Map<String, String>> currentDayWayBillStatistic(String courierId) {
		log.entry(courierId);
		List<Map<String, String>> list = null;
		
		/*首先从redis中获取各个状态的运单数量*/
		boolean redisEnable = false;
		try {
			String redisEnableStr = (String) PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.REDIS_ENABLE);
			log.debug("是否启用redis: "+redisEnableStr);
			redisEnable = Boolean.parseBoolean(redisEnableStr);
			if(redisEnable){
				list = countStatusNumByRedis(courierId);
				if (list != null && list.size() > 0) {
					return list;
				}
			}
		} catch (Exception e) {}
		try{
			long startTime = DateUtil.getTodayStartTime();

			long endTime = DateUtil.getTodayEndTime();
			log.entry("waybillDao.currentDayWaybillStatByCourier",courierId, startTime, endTime,WaybillType.OL_RETAILERS.getName());
		    list = (List<Map<String, String>>)waybillDao.currentDayWaybillStatByCourier(courierId, startTime, endTime,WaybillType.OL_RETAILERS.getName());
		    
		}catch(Exception e){
			log.error(e);
			throw new BaseSupportException(e);
		}
		
		handleCourierCountInfo(list);
		
		return log.exit(list);
	}

	private void handleCourierCountInfo(List<Map<String, String>> list){
		
		/*收件待配送*/
		Map<String, String> map1 = null;
		
		/*配送完成*/
		Map<String, String> map5 = null;
		
		/*拒收*/
		Map<String, String> mapf2 = null;
		
		/*回库*/
		Map<String, String> mapf1 = null;
		
		
		for(Map<String, String> map : list){
			int s = Integer.parseInt(String.valueOf(map.get("status")));
			switch (s) {
			case 1:
				map1 = map;
				break;
			case 5:
				map5 = map;
				break;
			case -1:
				mapf1 = map;
				break;
			case -2:
				mapf2 = map;
				break;
			default:
				break;
			}
		}
		
		list.clear();
		
		if(map1==null){
			map1 = new HashMap<String, String>();
			map1.put("status", "1");
			map1.put("num", "0");
		}
		list.add(map1);
		
		if(map5==null){
			map5 = new HashMap<String, String>();
			map5.put("status", "5");
			map5.put("num", "0");
		}
		list.add(map5);
		
		String error2 = "0";
		if(mapf2!=null){
			error2 = String.valueOf(mapf2.get("num"));
		}
		
		if(mapf1==null){
			mapf1 = new HashMap<String, String>();
			mapf1.put("status", "-1");
			mapf1.put("num", error2);
		}else{
			String error1 = String.valueOf(mapf1.get("num"));
			long num = Long.parseLong(error1)+Long.parseLong(error2);
			mapf1.put("num", String.valueOf(num));
		}
		
		list.add(mapf1);
	}
	
	
	/**
	 * 从配送员ID，从rendis中抓取当前该配送员所有的运单统计数据
	 * @param courierId
	 * @return
	 */
	private List<Map<String, String>> countStatusNumByRedis(String courierId){
		
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		try {

			/* 从redis缓存中获取各个状态的数量 */
			String todayStr = DateUtil.convertDateToString(DateUtil.getCurrentDate(),"yyyyMMdd");
//			String redisKey = courierId;

			/* 电商运单 仅涉及4个状态：1、取货配送(配送中)；5、完成配送; -1 回库；-2 拒收 */

			/* 收件配送 */
			Map<String, String> map1 = new HashMap<String, String>();
			map1.put("status", "1");
			
			long num1 = JedisUtil.getInstance().SETS.scard(REDIS_KEY_PREFIX+"_"+courierId + "_1_"+todayStr);
			map1.put("num", String.valueOf(num1));
			list.add(map1);

			/* 配送完成 */
			Map<String, String> map5 = new HashMap<String, String>();
			map5.put("status", "5");
			long num5 = JedisUtil.getInstance().SETS.scard(REDIS_KEY_PREFIX+"_"+courierId + "_5_"+todayStr);
			map5.put("num", String.valueOf(num5));
			list.add(map5);

			/*异常件(redis中 回库或者拒收的件会合并)*/
			Map<String, String> map0 = new HashMap<String, String>();
			map0.put("status", "-1");
			long numf1 = JedisUtil.getInstance().SETS.scard(REDIS_KEY_PREFIX+"_"+courierId + "_f1_"+todayStr);
			map0.put("num", String.valueOf(numf1));
			list.add(map0);
			
		} catch (Exception e) {
			return null;
		}
		return list;
	}
	
	
	@Override
	public void updateWaybillStatus(Map<String, String> paraMap) {
		// TODO Auto-generated method stub
		log.entry(paraMap);
		WayBillEntity entity = new WayBillEntity();
		/*快递员*/
		String courierId = paraMap.get("cid");
		entity.setCourier_id(courierId);
		
		/*快送宝运单ID(非电商ID)*/
		String ksbwaybillId = paraMap.get("id");
		if(!ksbwaybillId.startsWith("3km")){
			log.error("非法运单编号");
			throw new BaseSupportException("非法运单编号");
		}
		entity.setId(ksbwaybillId);
		
		/*运单状态*/
		String waybillStatus = paraMap.get("status");
		entity.setWaybill_status(waybillStatus);
		
		if(!waybillStatus.equals("1")){
			entity.setFinish_time(String.valueOf(new Date().getTime()));
		}
		
		/*实付金额(这个字段可以为空)*/
//        String fee = paraMap.get("fee");
//        entity.setFetch_buyer_fee(fee);
		
        /*备注(备注)*/
        String remarks = paraMap.get("remarks");
		entity.setRemarks(remarks);
        
		int statusInt = Integer.parseInt(waybillStatus);
		
		try{
		    waybillDao.updateWaybillById(entity);
		}catch(Exception e){
			log.error(e);
			throw new BaseSupportException(e);
		}
		/*配送完成或者改为异常件*/
		if(statusInt==5||statusInt<0){
			try{
				userDao.updateCourierDeliveryStatus(courierId, "0");
			}catch(Exception e){}
		}
		/*状态修改成功，配送记录表中 增加配送记录*/
		
	}

	@Override
	public Map<String, Object> queryWaybillByCourier(String courierId,
			String waybillId, String waybillStatus, int skip, int size) {
		// TODO Auto-generated method stub
		log.entry(courierId,waybillId,waybillStatus,skip,size);
		/*必须得参数快递员ID不能为空*/
		if(StringUtils.isBlank(courierId)){
			log.error("必须得参数：配送员编号[courierId] 为空");
			throw new BaseSupportException("必须得参数：配送员编号[courierId] 为空");
		}
		
		
		List<Map<String, String>> rsList = null;
		try{
			log.entry("waybillDao.queryCourierTodayOLWaybillList",courierId, waybillId, waybillStatus, WaybillType.OL_RETAILERS.getName(), skip, size);
			rsList = (List<Map<String, String>>) waybillDao.queryCourierTodayOLWaybillList(courierId, waybillId, waybillStatus, WaybillType.OL_RETAILERS.getName(), skip, size);
			DaoHelper.getCount();
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		rsMap.put("1", rsList);
		rsMap.put("2", DaoHelper.getCount());
		
		return log.exit(rsMap);
	}
	
	
}
