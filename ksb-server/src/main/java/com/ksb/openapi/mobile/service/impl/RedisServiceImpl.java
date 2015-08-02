package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;

import com.ksb.openapi.mobile.service.RedisService;
import com.ksb.openapi.util.JedisUtil;
import com.ksb.openapi.util.SystemConst;

import redis.clients.jedis.GeoParams;
import redis.clients.jedis.JedisPool;

@Service
public class RedisServiceImpl implements RedisService,InitializingBean{

	private Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	JedisPool jedisPool;
	
	/*记录配送员位置的前缀*/
	private String REDIS_GPSKEY_PREFIX = "COURIER_GPS_";
	
	/*判断redis是否宕机key*/
	private String REDIS_TESTKEY = "ksb-3km-test-key";
	
	/*配送员开工、收工状态*/
	private String REDIS_WORKSTATUS_PREFIX = "redis_workstatus_";
	
	/*配送员配送状态*/
	private String REDIS_PS_PREFIX = "redis_ps_status_";
	
	private String REDIS_UNALLOCATE_PREFIX = "redis_unallocate_";
	
	@Override
	public void afterPropertiesSet() throws Exception {
		JedisUtil.getInstance().setPool(jedisPool);
		
	}
	/* (non-Javadoc)
	 * @see com.ksb.openapi.mobile.service.impl.RedisService#recordCourierGps(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void recordCourierGps(String cid,String eid,String x,String y){
		
		log.entry(cid,eid,x,y);
		/*如果redis异常，直接跳过*/
		if(!redisIsWork()){
			return;
		}
		
		String redisKeyPrefix = REDIS_GPSKEY_PREFIX;
		try{
			/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
			redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_GEO_PREFIX);
		}catch(Exception e){}
		/*先从redis中删除配送员的位置(redis geo不允许覆盖)*/
		JedisUtil.getInstance().GEOS.zrem(redisKeyPrefix+eid, cid);
		
		/*把配送员的最新位置写入到redis中*/
		log.debug("write_to_redis_courier_gps",redisKeyPrefix+eid, Double.parseDouble(y), Double.parseDouble(x), cid);
		JedisUtil.getInstance().GEOS.geoset(redisKeyPrefix+eid, Double.parseDouble(y), Double.parseDouble(x), cid);
	}
	
	/* (non-Javadoc)
	 * @see com.ksb.openapi.mobile.service.impl.RedisService#updateWorkStatus(java.lang.String, java.lang.String)
	 */
	@Override
	public void updateWorkStatus(String cid,String workStatus){
		
		log.entry(cid,workStatus);
		if(!redisIsWork()){
			return;
		}
		String redisKeyPrefix = REDIS_WORKSTATUS_PREFIX;
		try {
			try{
				/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
				redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_WORKSTATUS_PREFIX);
			}catch(Exception e){}
			
			log.debug("update_redis_workstatus",redisKeyPrefix+cid,workStatus);
			
			JedisUtil.getInstance().STRINGS.set(redisKeyPrefix+cid, workStatus);
			
		} catch (Exception e) {}
	}
	
    /* (non-Javadoc)
	 * @see com.ksb.openapi.mobile.service.impl.RedisService#updateDeliveryStatus(java.lang.String, java.lang.String)
	 */
	@Override
	public void updateDeliveryStatus(String cid,String deliveryStauts){
		
		log.entry(cid,deliveryStauts);
		if(!redisIsWork()){
			return;
		}
		String redisKeyPrefix = REDIS_PS_PREFIX;
		try {
			try{
				/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
				redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_PS_STATUS_PREFIX);
			}catch(Exception e){}
			
			log.debug("update_redis_deliverystauts",redisKeyPrefix+cid,deliveryStauts);
			
			JedisUtil.getInstance().STRINGS.set(redisKeyPrefix+cid, deliveryStauts);
			
		} catch (Exception e) {}
	}
	
	
	/* (non-Javadoc)
	 * @see com.ksb.openapi.mobile.service.impl.RedisService#queryNearCourier(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> queryNearCourier(String eid,String x,String y){
		
		log.entry(eid,x,y);
		/*判断redis是否正常工作*/
		if(!redisIsWork()){
			return null;
		}
		
		List<String> courierList = new ArrayList<String>();
		/*查找半径(单位 km)*/
		String radiusStr = "3";
		try {
			radiusStr = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_QUERY_RADIUS);
		} catch (Exception e) {}

		/*存储配送员位置的 rediskey前缀*/
		String redisKeyPrefix = REDIS_GPSKEY_PREFIX;
		try{
			/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
			redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_GEO_PREFIX);
		}catch(Exception e){}
			 
		GeoParams geoParam = new GeoParams();
		/*设置查找半径(单位km)*/
		geoParam.radius(radiusStr);
		/*设置查询结果格式(只显示 配送员编号和距离)*/
		geoParam.geodistance();
		//geoParam.geojson();
		/*查找的结果按距离从近到远排序*/
		geoParam.asc();
			
		log.debug("geoscanByRadius",redisKeyPrefix+eid, Double.parseDouble(y), Double.parseDouble(x));
		
		List<String> redisList = null;
		try{
			redisList = JedisUtil.getInstance().getJedis().geoscanByRadius(redisKeyPrefix+eid, Double.parseDouble(y), Double.parseDouble(x), geoParam);
		}catch(Exception e){
			return null;
		}
		
		log.debug("redis中查到配送公司["+eid+"];半径["+radiusStr+"]公里内的配送员 ",redisList);
		if(redisList==null||redisList.size()==0){
			log.debug("redis中查到配送公司["+eid+"];半径["+radiusStr+"]公里内 无可用的配送员");
			/*附近没有配送员*/
			return courierList;
		}
			
		/*从redis中获取的数据格式中提取配送员编号和经纬度信息*/
		for(String rk : redisList){
			String splitRs[] = rk.split("\\^");
			if(splitRs==null||splitRs.length<2){
				/*数据获取异常*/
				continue;
			}
				
			String cid = splitRs[0];
			
			/*开工收工状态(默认是收工)*/
			String workStatus = getWorkStatus(REDIS_WORKSTATUS_PREFIX+cid);
			if(StringUtils.isBlank(workStatus)){
				workStatus = "2";
			}
			
			/*配送状态(默认是可接受新订单)*/
			String psStatus = getDeliveryStatus(REDIS_PS_PREFIX+cid);
			if(StringUtils.isBlank(psStatus)){
				psStatus = "0";
			}
				
			/*需要判断配送员的开工状态和配送状态，只要获取开工、并且当前未配送的的
			 * 从redis中获取该配送员的 开工状态和配送状态
			 * */
			if(workStatus.equals("1")){
				if(psStatus.equals("0")){
					//String distance = splitRs[1];
					log.debug("选中的配送员",cid);
					courierList.add(cid);
					/*找到第一个最近 配送员即可*/
					break;
				}
			}
		}
		
	return courierList;
	}
	
	/**
	 * 配送员开工/收工状态
	 * @param cid
	 * @return
	 */
	private String getWorkStatus(String cid){
		log.entry(cid);
		/*如果redis异常，直接跳过*/
		if(!redisIsWork()){
			return null;
		}
		String redisKeyPrefix = REDIS_WORKSTATUS_PREFIX;
		String workStatus = null;
		try {
			try{
				/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
				redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_WORKSTATUS_PREFIX);
			}catch(Exception e){}
			try{
				workStatus = JedisUtil.getInstance().STRINGS.get(redisKeyPrefix+cid);
			}catch(Exception e){
				return null;
			}
			
		} catch (Exception e) {}
		
		return workStatus;
	}
	
	/**
	 * 配送员配送状态
	 * @param cid
	 * @return
	 */
	private String getDeliveryStatus(String cid){
		log.entry(cid);
		if(!redisIsWork()){
			return null;
		}
		String redisKeyPrefix = REDIS_PS_PREFIX;
		String workStatus = null;
		try {
			try{
				/*防止从配置文件中读取数据出错，而无法使用默认的key值*/
				redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_PS_STATUS_PREFIX);
			}catch(Exception e){}
			try{
				workStatus = JedisUtil.getInstance().STRINGS.get(redisKeyPrefix+cid);
			}catch(Exception e){
				return null;
			}
			
		} catch (Exception e) {}
		
		return workStatus;
	}
	
	
	public void saveUnAllocateWaybill(String cityCode,String waybillId){
		String redisKeyPrefix = REDIS_UNALLOCATE_PREFIX;
		
		try{
			redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.UNALLOCATE_PREFIX);
			log.entry(redisKeyPrefix,cityCode,waybillId);
			JedisUtil.getInstance().SETS.sadd(redisKeyPrefix, cityCode+"^"+waybillId);
		}catch(Exception e){
			log.error(e);
		}
	}
	
	public void getUnAllocateWaybill(){
		
		String redisKeyPrefix = REDIS_UNALLOCATE_PREFIX;
		
		try{
			redisKeyPrefix = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.UNALLOCATE_PREFIX);
			log.entry(redisKeyPrefix);
		}catch(Exception e){
			log.error(e);
		}
		
		long countNum = JedisUtil.getInstance().SETS.scard(redisKeyPrefix);
		
		/*存在未分配的订单*/
		if(countNum > 0){
//			JedisUtil.getInstance().SETS
		}
		
	}
	
	
	
	
	
	/**
	 * 判断redis是否宕机
	 * @return
	 */
	public boolean redisIsWork(){
//		JedisUtil.getInstance().setPool(jedisPool);
		boolean redisWork = false;
		try{
			String kv = "3km"+new Random().nextInt();
			JedisUtil.getInstance().STRINGS.set(REDIS_TESTKEY, kv);
			String redisKv = JedisUtil.getInstance().STRINGS.get(REDIS_TESTKEY);
			if(redisKv!=null){
				if(redisKv.equals(kv)){
					redisWork = true;
				}
			}
		}catch(Exception e){}
		return log.exit(redisWork);
	}
	
	
}
