package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.sharding.dao.DaoHelper;

import com.esotericsoftware.minlog.Log;
import com.ksb.openapi.dao.EnterpriseDao;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.em.WaybillType;
import com.ksb.openapi.entity.EnterpriseCityEntity;
import com.ksb.openapi.entity.EnterpriseEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.ShipperUserEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.ShipperService;
import com.ksb.openapi.util.AddressUtils;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.DesUtil;
import com.ksb.openapi.util.MD5Util;

@Service
public class ShipperServiceImpl implements ShipperService {

	private Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	EnterpriseDao enterpriseDao = null;
	
	@Autowired
	UserDao userDao = null;
	
	@Autowired
	WaybillDao waybillDao = null;
	
	@Override
	public List<ShipperEntity> queryShipperList(Map<String, String> paraMap) {
		
		/*未提供任何查询参数*/
		if(paraMap==null){
			return null;
		}
		
		List<ShipperEntity> rsList = null;
		
		/*前期支持id、name模糊查询、类型查询*/
		String id = paraMap.get("id");
		String name = paraMap.get("name");
		String shipperType = paraMap.get("t");
		
		ShipperEntity entity = new ShipperEntity();
		entity.setId(id);
		if(StringUtils.isNotBlank(name)){
			entity.setName("%"+name+"%");
		}
		entity.setShipper_type(shipperType);
		
		try{
		    rsList = (List<ShipperEntity>)enterpriseDao.queryShipperList(entity);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		return rsList;
	}

	@Override
	public void createShipper(ShipperEntity shipperEntity) {
		// TODO Auto-generated method stub
       throw new BaseSupportException("该方法未写任何实现");
	}

	@Override
	public void queryShipperById(String id) {
		// TODO Auto-generated method stub
		throw new BaseSupportException("该方法未写任何实现");
	}

	@Override
	public List<Map<String, String>> shipperCommonAddress(String spId, String cityName) {
		// TODO Auto-generated method stub

		if(StringUtils.isBlank(spId)){
			throw new BaseSupportException("未指定商家");
		}
		
		List<Map<String, String>> rsList = null;
		try{
			rsList = (List<Map<String,String>>)enterpriseDao.queryShipperAddress(spId,null);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		return rsList;
	}

	@Override
	public ResultEntity authenSpUser(String un, String pwd) {
		// TODO Auto-generated method stub
		log.entry(un,pwd);
		ResultEntity rsEntity = new ResultEntity();
		ShipperUserEntity spUserEntity = null;
		try{
			spUserEntity = (ShipperUserEntity)userDao.authenShipperUser(un, MD5Util.MD5(MD5Util.MD5(pwd)));
		}catch(Exception e){
			Log.error("快递员登录接口系统异常",e);
			rsEntity.setErrors("ER");
			rsEntity.setObj("系统异常,登录失败");
			return rsEntity;
		}
		if(spUserEntity==null){
			rsEntity.setErrors("ER");
			rsEntity.setObj("用户名或者密码不对");
			return rsEntity;
		}
		
		String status = spUserEntity.getStatus();
		/*如果没有获取到状态字段，则无法登陆*/
		if(StringUtils.isBlank(status)){
			rsEntity.setErrors("ER");
			rsEntity.setObj("账号被停用");
			return rsEntity;
		}
		
		/*状态非0 表示异常*/
		if(!status.equals("0")){
			rsEntity.setErrors("ER");
			rsEntity.setObj("账号被锁定");
			return rsEntity;
		}
		
		rsEntity.setSuccess(true);
		rsEntity.setErrors("OK");
		rsEntity.setObj(spUserEntity);
		return log.exit(rsEntity);
	}

	@Override
	public ShipperUserEntity updateShipperDefualtAddress(ShipperUserEntity entity){
		
		if(StringUtils.isBlank(entity.getSp_id())){
			throw new BaseSupportException("商家编号为空");
		}
		if(StringUtils.isBlank(entity.getSp_name())){
			throw new BaseSupportException("商家名称为空");
		}
		if(StringUtils.isBlank(entity.getAddress())){
			throw new BaseSupportException("商家地址为空");
		}
		if(StringUtils.isBlank(entity.getCity_name())){
			throw new BaseSupportException("未指定城市");
		}
		if(StringUtils.isBlank(entity.getAddress_detail())){
			entity.setAddress_detail("");
		}
		/*经纬度为空，说明手机定位失败，需要通过地理编码计算一下经纬度*/
		if(StringUtils.isBlank(entity.getAddress_x()) || StringUtils.isBlank(entity.getAddress_y())){
			ResultEntity rs = null;
			try{
			    rs = AddressUtils.validateAddressByBDMap(entity.getAddress()+entity.getAddress_detail(),entity.getCity_name());
			}catch(Exception e){
				throw new BaseSupportException(e);
			}
			String obj = rs.getObj().toString();
			String coors[] = obj.split(";");
			entity.setAddress_x(coors[0]);
			entity.setAddress_y(coors[1]);
		}
		
		try{
			log.debug("enterpriseDao.updateShipperAddress",entity.getSp_id(),entity.getSp_name(), entity.getProvince_name(),entity.getCity_name(),entity.getAddress(), entity.getAddress_detail(),entity.getCity_code(), entity.getAddress_x(), entity.getAddress_y());
			enterpriseDao.updateShipperAddress(entity.getSp_id(),entity.getSp_name(), entity.getProvince_name(),entity.getCity_name(),entity.getAddress(), entity.getAddress_detail(),entity.getCity_code(), entity.getAddress_x(), entity.getAddress_y());
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		return entity;
	}

	@Override
	public void createShipperAndUser(ShipperUserEntity entity) {
		// TODO Auto-generated method stub
		if(entity==null){
			throw new BaseSupportException("未提供任何参数");
		}
		
		if(StringUtils.isBlank(entity.getSp_name())){
			throw new BaseSupportException("商家名称为空");
		}
		
//		if(StringUtils.isBlank(entity.getAddress())){
//			throw new BaseSupportException("地址为空");
//		}
		
//		if(StringUtils.isBlank(entity.getCity_name())){
//			throw new BaseSupportException("未指定城市");
//		}
		
//		if(StringUtils.isBlank(entity.getAddress_detail())){
//			entity.setAddress_detail("");
//		}
		
		/*经纬度为空，说明手机定位失败，需要通过地理编码计算一下经纬度*/
//		if(StringUtils.isBlank(entity.getAddress_x()) || StringUtils.isBlank(entity.getAddress_y())){
//			ResultEntity rs = null;
//			try{
//			    rs = AddressUtils.validateAddressByBDMap(entity.getAddress()+entity.getAddress_detail(),entity.getCity_name());
//			}catch(Exception e){
//				throw new BaseSupportException(e);
//			}
//			String obj = rs.getObj().toString();
//			String coors[] = obj.split(";");
//			entity.setAddress_x(coors[0]);
//			entity.setAddress_y(coors[1]);
//		}
		
        if(StringUtils.isBlank(entity.getName())){
        	throw new BaseSupportException("登录用户名为空");
        }
        if(StringUtils.isBlank(entity.getPhone())){
        	throw new BaseSupportException("手机号为空");
        }
        if(StringUtils.isBlank(entity.getPasswd())){
        	throw new BaseSupportException("密码为空");
        }
//        if(StringUtils.isBlank(entity.getReal_name())){
//        	throw new BaseSupportException("联系人为空");
//        }
        if(StringUtils.isBlank(entity.getNick_name())){
        	entity.setNick_name("");
        }
        
        
        /*创建商家基本信息、地址信息*/
        try{
        	entity.setSp_id(String.valueOf(DaoHelper.createPrimaryKey()));
        	enterpriseDao.createShipper(entity,entity.getEnterprise_id());
        }catch(Exception e){
        	throw new BaseSupportException(e);
        }
        
        /*创建商家登录用户信息*/
        try{
        	entity.setUid(String.valueOf(DaoHelper.createPrimaryKey()));
        	entity.setStatus("0");
        	
        	/*设置密码相关*/
        	String originPasswd = entity.getPasswd();
        	entity.setPasswd(MD5Util.MD5(MD5Util.MD5(originPasswd)));
        	entity.setSrc_passwd(DesUtil.getInstance().encrypt(originPasswd));
        	
        	userDao.createShipperUser(entity);
        }catch(Exception e){
        	throw new BaseSupportException(e);
        }
		
	}
	
	
	//@Override
	public ResultEntity updateShipperUserPasswd(String userName, String oldPasswd,
			String newPasswd) {
	
		/*原始密码,使用自定义的可逆加密算法存储*/
		String srcPwd = newPasswd;
		try {
			srcPwd = DesUtil.getInstance().encrypt(newPasswd);
		} catch (Exception e1) {}
		
		/*密码使用两次MD5加密*/
		String op = MD5Util.MD5(MD5Util.MD5(oldPasswd));
		String np = MD5Util.MD5(MD5Util.MD5(newPasswd));
		
		ShipperUserEntity shipperUserEntity = null;
		
		ResultEntity rsEntity = new ResultEntity();
		rsEntity.setSuccess(false);
		try {
			shipperUserEntity = (ShipperUserEntity)userDao.authenShipperUser(userName, op);
			if(shipperUserEntity==null){
				rsEntity.setErrors("ER");
				rsEntity.setObj("旧密码不正确");
				return rsEntity;
			}
			userDao.updateShipperUserPasswd(userName, op, np, srcPwd);
		} catch (Exception e) {
			rsEntity.setErrors("ER");
			rsEntity.setObj("系统异常");
			return rsEntity;
		}
		
		rsEntity.setSuccess(true);
		rsEntity.setErrors("OK");
		return rsEntity;
	}

	@Override
	public Map<String, Object> queryWaybillByShipper(String shipperId,
			String waybillId, String waybillStatus, int skip, int size) {

		log.entry(shipperId,waybillId,waybillStatus,skip,size);
		/*必须得参数快递员ID不能为空*/
		if(StringUtils.isBlank(shipperId)){
			log.error("商家编号为空");
			throw new BaseSupportException("商家编号为空");
		}
		
		List<Map<String, String>> rsList = null;
		try{
			long startTime = DateUtil.getTodayStartTime();
			long endTime = DateUtil.getTodayEndTime();
			log.entry("请求waybillDao.queryShipperO2OWaybillList: 参数：",shipperId, waybillId, waybillStatus, WaybillType.O2O.getName(),startTime,endTime, skip, size);
			
			rsList = (List<Map<String, String>>)waybillDao.queryShipperO2OWaybillList(shipperId, waybillId, waybillStatus, WaybillType.O2O.getName(),startTime,endTime,skip, size);
		    log.entry("waybillDao.queryShipperO2OWaybillList返回结果: "+rsList);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		
		/*增加等待时间属性*/
		addWaitTime(rsList);
		rsMap.put("1", rsList);
		rsMap.put("2", DaoHelper.getCount());
		
		return log.exit(rsMap);
	}
	
	public void addWaitTime(List<Map<String, String>> list) {

		for (Map<String, String> map : list) {
			String waitTime = "0";
			String sysNotions = "";
			try {
				String creatTime = map.get("create_time");
				long currentTime = new Date().getTime();
				double wt = currentTime- DateUtil.convertStringToLong(creatTime,DateUtil.timePattern);
				java.text.DecimalFormat df = new java.text.DecimalFormat("#0.0");
				double wtime = wt / 1000 / 60;

				waitTime = df.format(wtime);
				/*超过一个小时提醒*/
				if(wtime>60){
					sysNotions = "您的订单等待已经超过一个小时";
				}
				
				/*超过两个小时,建议取消*/
				if(wtime>120){
					sysNotions = "等待时间过长,您的地址可能不在我们的配送范围内,建议您取消该订单!";
				}
			} catch (Exception e) {
			}
			map.put("wait_time", waitTime+" 分钟");
			map.put("sys_notification", sysNotions);
		}
	}
	
	
	@Override
	public List<Map<String, String>> currentDayShipperWayBillStatistic(String shipperId) {
		
		log.entry(shipperId);
		List<Map<String, String>> list = null;

		try{
			long startTime = DateUtil.getTodayStartTime();
			//long startTime = 0;
			long endTime = DateUtil.getTodayEndTime();
			log.entry("waybillDao.currentDayWaybillStatByShipper",shipperId, startTime, endTime,WaybillType.O2O.getName());
		    list = (List<Map<String, String>>)waybillDao.currentDayWaybillStatByShipper(shipperId, startTime, endTime,WaybillType.O2O.getName());
		    
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		return log.exit(handleShipperCountInfo(list));
	}
	
	private List<Map<String, String>> handleShipperCountInfo(List<Map<String, String>> list){
		
		try {
			/* 所有状态码先都置为0(相当于模板) */
			Map<String, Map<String, String>> tm = getDefaultStatMap();

			Map<String, Map<String, String>> tmpMap = new HashMap<String, Map<String, String>>();
			for (Map<String, String> m : list) {
				String statusCode = String.valueOf(m.get("status"));
				m.put("status", statusCode);
				m.put("num", String.valueOf(m.get("num")));
				tmpMap.put(statusCode, m);
			}

			tm.putAll(tmpMap);

			/* 单独处理拒收和回库件 */

			long e1 = Long.parseLong(String.valueOf(tm.get("-1").get("num")));
			long e2 = Long.parseLong(String.valueOf(tm.get("-2").get("num")));

			tm.get("-1").put("num", String.valueOf(e1 + e2));
			tm.remove("-2");

			list.clear();

			list = new ArrayList<Map<String, String>>(tm.values());
			return list;
		} catch (Exception e) {
			return list;
		}
	}
	
	/**
	 * 获取默认的统计项(防止数据库中没有某一个状态值)
	 * @return
	 */
	private Map<String, Map<String, String>> getDefaultStatMap(){
		
		Map<String, Map<String, String>> rsMap = new HashMap<String, Map<String,String>>();
		
		for(int i=-2;i<=5;i++){
			Map<String, String> tm = new HashMap<String, String>();
			tm.put("status", String.valueOf(i));
			tm.put("num", "0");
			rsMap.put(String.valueOf(i), tm);
		}
		
		return rsMap;
	}
	
	
	/**
	 * 运单入库
	 */
	@Override
	public void createWaybill(Map<String, String> paraMap) {
		// TODO Auto-generated method stub

		log.entry(paraMap);
		if(paraMap==null||paraMap.size()==0){
			log.error("paraMap为空,未提供任何参数");
			throw new BaseSupportException("未提供任何参数");
		}
		
		/*前期开发，需要知道存储那些字段，业务成熟以后以后优化，通过反射把map对象转换为Waybillentity对象*/
		String shipperId = paraMap.get("sp_id");
		if(StringUtils.isBlank(shipperId)){
			log.error("商家编号为空");
			throw new BaseSupportException("商家编号为空");
		}
		String cityCode = paraMap.get("city_code");
		if(StringUtils.isBlank(cityCode)){
			log.error("城市名为空");
			throw new BaseSupportException("城市名为空");
		}		
		
		String x = paraMap.get("x");
		String y = paraMap.get("y");
		if(StringUtils.isBlank(x)||StringUtils.isBlank(y)){
			log.error("无法获取商家位置");
			throw new BaseSupportException("无法获取商家位置");
		}
		
        String remarks = paraMap.get("remarks");
        if(StringUtils.isBlank(remarks)){
        	paraMap.put("remarks", "");
        }
        
        String cargoType = paraMap.get("cargo_type");
        String cargoNum = paraMap.get("cargo_num");
        String waybillNum = paraMap.get("waybill_num");
        
        String courierId = paraMap.get("cid");
        if(StringUtils.isBlank(courierId)){
			log.debug("该订单无配送员，需要配送公司手工干预");
			//throw new BaseSupportException("无配送员");
        }
        
        WayBillEntity waybillEntity = new WayBillEntity();
        
        waybillEntity.setCity_code(cityCode);
        /*物品信息*/
        waybillEntity.setCargo_type(cargoType);
        waybillEntity.setCargo_num(cargoNum);
        
        /*一票多单*/
        waybillEntity.setWaybill_num(waybillNum);
        
        /*配送员*/
        waybillEntity.setCourier_id(courierId);
        /*运单隶属的电商*/
        waybillEntity.setShippers_id(shipperId);
        /*到付应收金额*/
        waybillEntity.setCargo_price("0");
        /*是否需要到付*/	
        waybillEntity.setIs_topay("0");
        /*备注*/
        waybillEntity.setRemarks(remarks);
        
        /*默认使用快送宝配送平台*/
        String delivery_eid_id = paraMap.get("delivery_eid_id");
        if(StringUtils.isBlank(delivery_eid_id)){
        	//delivery_eid_id = "100";
        	throw new BaseSupportException("该区域无可用的配送员");
        }
        waybillEntity.setThird_platform_id(delivery_eid_id);
        /*运单状态(待接单、待取、配送中、完成、异常件)*/
        String status = paraMap.get("status");
        if(StringUtils.isBlank(status)){
        	status = "0";
        }
        waybillEntity.setWaybill_status(status);
        waybillEntity.setPayment_status("0");
        waybillEntity.setWaybill_type(WaybillType.O2O.getName());
        try {
			waybillEntity.setId("3km-"+DaoHelper.createPrimaryKey().toString());
		} catch (Exception e1) {throw new BaseSupportException(e1);}
        
        List<WayBillEntity> list = new ArrayList<WayBillEntity>();
        list.add(waybillEntity);
		
        try{
		    waybillDao.batchCreateKSBWayBill(list);
		    
		    /*更新配送员的状态 为正在配送（正在配送的，不能继续接受新单）
		     * 更新courier表的delivery_status字段值(0表示 未配送；1表示配送中)
		     * 配送完毕，修改delivery_status为1
		     * */
		    if(StringUtils.isNotBlank(courierId)){
		    	userDao.updateCourierDeliveryStatus(courierId, "1");
		    }else{
		    	/*说明该订单没有配送员，进入未分配表 unallocate_waybill table中*/
		    	
		    	new AsynSaveUnallocateWaybill(waybillEntity.getId(), cityCode, x, y, waybillEntity.getThird_platform_id()).run();
		    }
		    
        }catch(Exception e){
        	log.error(e.getMessage());
        	throw new BaseSupportException(e);
        }
	
        /*优化项：1、入库后的运单，在redis中记录，作为之后统计的依据*/
	}
	
	/**
	 * 异步写入未自动分配的订单
	 * @author houshipeng
	 *
	 */
	class AsynSaveUnallocateWaybill extends Thread{
		String wbId;
		String cityCode;
		String x;
		String y;
		String eid;
		public AsynSaveUnallocateWaybill(String wbId,String cityCode,String x,String y,String eid){
			this.wbId = wbId;
			this.cityCode = cityCode;
			this.x = x;
			this.y = y;
			this.eid = eid;
		}
		
		public void run(){
			try{
				waybillDao.saveUnallocateWaybill(wbId, cityCode, x, y, eid);
			}catch(Exception e){
				log.error("异步写入未分配的订单异常: "+e);
			}
			
		}
		
	}

	
	
	
	/**
	 * 根据城市信息 获取负责该城市配送的企业
	 * @param cityName
	 * @param adcode
	 * @param cityCode
	 * @param district
	 * @return
	 */
	public List<EnterpriseCityEntity> getEnterpriseAreaByCityInfo(String cityName,String adcode,String cityCode,String district){
		
		List<EnterpriseCityEntity> list = null;
		if(StringUtils.isBlank(cityCode)){
			throw new BaseSupportException("citycode为空");
		}
		try{
			list = (List<EnterpriseCityEntity>)enterpriseDao.queryEnterpriseByCityInfo(null, null, cityCode, null);
		}catch(Exception e){
			throw new BaseSupportException(e.getMessage());
		}
		
		return list;
	}
	
	@Override
	public Map<String, Object> querySpUser(String eid, String spName, int skip, int size) {
		// TODO Auto-generated method stub
		
		log.entry(eid,spName,skip,size);
		/*必须得参数快递员ID不能为空*/
		if(StringUtils.isBlank(eid)){
			log.error("配送公司编号为空");
			throw new BaseSupportException("配送公司编号为空");
		}
		
		List<ShipperUserEntity> rsList = null;
		try{
			log.entry("请求userDao.querySpUser: 参数：",eid, spName, skip, size);
			if(StringUtils.isNotBlank(spName)){
				spName = "%"+spName+"%";
			}
			rsList = (List<ShipperUserEntity>)userDao.querySpUser(eid, spName, skip, size);
			
		    log.entry("userDao.querySpUser返回结果: "+rsList);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		
		rsMap.put("1", rsList);
		rsMap.put("2", DaoHelper.getCount());
		
		return log.exit(rsMap);
	}

	@Override
	public void createEnterprise(EnterpriseEntity enterprise) {
		// TODO Auto-generated method stub
		
		if(enterprise==null){
			throw new BaseSupportException("参数为空");
		}
		if(StringUtils.isBlank(enterprise.getCity())){
			throw new BaseSupportException("城市为空");
		}
		if(StringUtils.isBlank(enterprise.getName())){
			throw new BaseSupportException("公司名称为空");
		}		
		if(StringUtils.isBlank(enterprise.getContact())){
			throw new BaseSupportException("联系人为空");
		}
		if(StringUtils.isBlank(enterprise.getTel())){
			throw new BaseSupportException("联系电话为空");
		}
		
		try{
			enterpriseDao.createEnterprise(enterprise);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}

	@Override
	public void createShipper(ShipperEntity spEntity, String eid) {
		// TODO Auto-generated method stub
		
		if(spEntity==null){
			throw new BaseSupportException("参数为空");
		}
		if(StringUtils.isBlank(spEntity.getCity())){
			throw new BaseSupportException("城市为空");
		}
		if(StringUtils.isBlank(spEntity.getName())){
			throw new BaseSupportException("名称为空");
		}		
		if(StringUtils.isBlank(spEntity.getContact())){
			throw new BaseSupportException("联系人为空");
		}
		if(StringUtils.isBlank(spEntity.getTel())){
			throw new BaseSupportException("联系电话为空");
		}
		
		try{
			enterpriseDao.createShipper(spEntity, eid);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}
	
	@Override
	public void shipperHandleWaybill(String spId,String spUserId,String waybillId,String status){
		
		log.entry(spId,spUserId,waybillId,status);
		
		if(StringUtils.isBlank(spId)){
			throw new BaseSupportException("商家编号为空");
		}
		if(StringUtils.isBlank(waybillId)){
			throw new BaseSupportException("订单编号为空");
		}
		if(StringUtils.isBlank(status)){
			throw new BaseSupportException("未知的操作");
		}
		
		try{
			waybillDao.updateWaybillStatusByShipper(spId, spUserId, waybillId, status);
		}catch(Exception e){
			log.error(e);
			throw new BaseSupportException(e);
		}
		
	}
	
}
