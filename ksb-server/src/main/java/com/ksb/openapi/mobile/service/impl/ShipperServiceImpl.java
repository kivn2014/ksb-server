package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import com.ksb.openapi.entity.ShipperAddressEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.ShipperUserEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.CourierService;
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
	
	@Autowired
	CourierService courierService;
	
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
	public List<ShipperAddressEntity> queryShipperAddressList(String sp_id){
		
		/*商家编号为空*/
		if(StringUtils.isBlank(sp_id)){
			throw new BaseSupportException("商家编号空为");
		}
		
		List<ShipperAddressEntity> rsList = null;
		try{
			rsList = (List<ShipperAddressEntity>)enterpriseDao.queryShipperAddressList(sp_id);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		return rsList;
	}

	@Override
	public void shipperDefaultAddress(String sp_id,String address_id){
		
		log.entry("request param:",sp_id,address_id);
		
		if(StringUtils.isBlank(sp_id)){
			throw new BaseSupportException("商家编号为空");
		}
		if(StringUtils.isBlank(address_id)){
			throw new BaseSupportException("未指定商家地址");
		}
		
		try{
			/*先重置商家所有的发货地址为 非默认地址*/
			enterpriseDao.resetShipperDefaultAddress(sp_id);
			
			/*设置指定的地址为商家默认发货地址*/
			enterpriseDao.shipperDefaultAddress(sp_id, address_id);
			
			/*把设置的默认地址拷贝到shipper表中(涉及字段:地址、门牌、经纬度、联系人信息)*/
			enterpriseDao.copyDefault2Shippers(sp_id, address_id);
			
			log.entry("set default address sucess");
		}catch(Exception e){
			log.error("请求异常："+e.getMessage());
			throw new BaseSupportException(e);
		}
	}
	
	@Override
	public void cancelShipperAddress(String sp_id,String address_id){
		if(StringUtils.isBlank(sp_id)){
			throw new BaseSupportException("商家编号为空");
		}
		if(StringUtils.isBlank(address_id)){
			throw new BaseSupportException("未指定商家地址");
		}
		
		try{
			
			ShipperAddressEntity entity = (ShipperAddressEntity)enterpriseDao.queryShipperAddressById(sp_id, address_id);
			/*商家下得地址不存在*/
			if(entity==null){
				throw new BaseSupportException("要删除的地址不存在");
			}
			String isDefaultAddress = entity.getIs_default();
			/*当前默认地址不能删除*/
			if("1".equals(isDefaultAddress)){
				throw new BaseSupportException("默认地址不能删除");
			}
			enterpriseDao.cancelShipperAddress(sp_id, address_id);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
	}
	
	
	public void editShipperAddress(ShipperAddressEntity entity){
		
		if(entity==null){
			throw new BaseSupportException("无任何参数");
		}
		
		/*商家编号*/
		if(StringUtils.isBlank(entity.getSp_id())){
			throw new BaseSupportException("商家编号为空");
		}
		
		/*商家地址编号*/
		if(StringUtils.isBlank(entity.getId())){
			throw new BaseSupportException("地址编号为空");
		}
		
		/*地址信息*/
		if(StringUtils.isBlank(entity.getAddress())){
			throw new BaseSupportException("地址为空");
		}
		
		/*联系人信息*/
		if(StringUtils.isBlank(entity.getContact())){
			throw new BaseSupportException("联系人为空");
		}
		
		/*联系人手机号*/
		if(StringUtils.isBlank(entity.getPhone())){
			throw new BaseSupportException("联系人手机号为空");
		}
		
		/*地址对应经纬度信息*/
		if(StringUtils.isBlank(entity.getAddress_x()) || StringUtils.isBlank(entity.getAddress_y())){
			throw new BaseSupportException("地址经纬度信息为空");
		}
		
		/*citycode*/
		if(StringUtils.isBlank(entity.getCity_code())){
			throw new BaseSupportException("city_code为空");
		}
		
		/*省市区*/
		if(StringUtils.isBlank(entity.getProvince_name())){
			throw new BaseSupportException("省信息为空");
		}
		
		if(StringUtils.isBlank(entity.getCity_name())){
			throw new BaseSupportException("市信息为空");
		}
		
		if(StringUtils.isBlank(entity.getDistrict_name())){
			throw new BaseSupportException("区/县信息为空");
		}
		
		if(StringUtils.isBlank(entity.getIs_default())){
			entity.setIs_default("0");
		}
		try{
			enterpriseDao.editShipperAddress(entity);
			
			/*设置为默认地址*/
			if(entity.getIs_default().equals("1")){
				this.shipperDefaultAddress(entity.getSp_id(), entity.getId());
			}
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}
	
	@Override
	public void addShipperAddress(ShipperAddressEntity entity){
		
		if(entity==null){
			throw new BaseSupportException("无任何参数");
		}
		
		/*商家编号*/
		if(StringUtils.isBlank(entity.getSp_id())){
			throw new BaseSupportException("商家编号为空");
		}
		
		/*地址信息*/
		if(StringUtils.isBlank(entity.getAddress())){
			throw new BaseSupportException("地址为空");
		}
		
		/*联系人信息*/
		if(StringUtils.isBlank(entity.getContact())){
			throw new BaseSupportException("联系人为空");
		}
		
		/*联系人手机号*/
		if(StringUtils.isBlank(entity.getPhone())){
			throw new BaseSupportException("联系人手机号为空");
		}
		
		/*地址对应经纬度信息*/
		if(StringUtils.isBlank(entity.getAddress_x()) || StringUtils.isBlank(entity.getAddress_y())){
			throw new BaseSupportException("地址经纬度信息为空");
		}
		
		/*citycode*/
		if(StringUtils.isBlank(entity.getCity_code())){
			throw new BaseSupportException("city_code为空");
		}
		
		/*省市区*/
		if(StringUtils.isBlank(entity.getProvince_name())){
			throw new BaseSupportException("省信息为空");
		}
		
		if(StringUtils.isBlank(entity.getCity_name())){
			throw new BaseSupportException("市信息为空");
		}
		
		if(StringUtils.isBlank(entity.getDistrict_name())){
			throw new BaseSupportException("区/县信息为空");
		}
		
		/*新添加的发单地址,如果未指定是否为默认地址，默认值调为非默认地址*/
		if(StringUtils.isBlank(entity.getIs_default())){
			entity.setIs_default("0");
		}
		
		try{
			long addressId = DaoHelper.createPrimaryKey();
			entity.setId(addressId+"");
			enterpriseDao.createShipperAddress(entity);
			
			/*新添加的地址需要设置为默认地址*/
			if(entity.getIs_default().equals("1")){
//				enterpriseDao.resetShipperDefaultAddress(entity.getSp_id());
//				enterpriseDao.shipperDefaultAddress(entity.getSp_id(), entity.getId());
//				enterpriseDao.copyDefault2Shippers(entity.getSp_id(), entity.getId());
				
				this.shipperDefaultAddress(entity.getSp_id(), entity.getId());
			}
			
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
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
			log.error("citycode为空");
			throw new BaseSupportException("citycode为空");
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
//        if(StringUtils.isBlank(courierId)){
//			log.debug("该订单无配送员，需要配送公司手工干预");
//			//throw new BaseSupportException("无配送员");
//        }
        
        WayBillEntity waybillEntity = new WayBillEntity();
        
        waybillEntity.setCity_code(cityCode);
        /*物品信息*/
        waybillEntity.setCargo_type(cargoType);
        waybillEntity.setCargo_num(cargoNum);
        
        /*一票多单*/
        waybillEntity.setWaybill_num(waybillNum);
        
        /*运单隶属的电商*/
        waybillEntity.setShippers_id(shipperId);
        /*到付应收金额*/
        String cargoPrice = paraMap.get("cargo_price");
        if(StringUtils.isBlank(cargoPrice)){
        	cargoPrice = "0";
        }
        waybillEntity.setCargo_price(cargoPrice);
        /*是否需要到付*/	
        waybillEntity.setIs_topay("0");
        /*备注*/
        waybillEntity.setRemarks(remarks);
        
        waybillEntity.setCity_name(paraMap.get("city_name"));
        
        /*配送员*/
//        waybillEntity.setCourier_id(courierId);
//        
//        /*默认使用快送宝配送平台*/
//        String delivery_eid_id = paraMap.get("delivery_eid_id");
//        if(StringUtils.isBlank(delivery_eid_id)){
//        	throw new BaseSupportException("该区域无可用的配送员");
//        }
//        waybillEntity.setThird_platform_id(delivery_eid_id);
        
        
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
       // list.add(waybillEntity);
        
        int wnum = Integer.parseInt(waybillEntity.getWaybill_num());
        
        List<String> psList = new ArrayList<String>();
        /*订单需求总数转化为实际订单数*/
        for(int i=1;i<=wnum;i++){
        	try{
        		WayBillEntity wb = waybillEntity.clone();
        		String id = null;
        		try{
        			id = "3km-"+DaoHelper.createPrimaryKey().toString();
        			wb.setId(id);
        			wb.setWaybill_num("1");
        		}catch(Exception e){}
        		
        		list.add(wb);
        		psList.add(id);
        	}catch(Exception e){
        		throw new BaseSupportException("订单创建失败,请稍后重试");
        	}
        }
      
        try{
		    waybillDao.batchCreateKSBWayBill(list);
		    list.clear();
		    /*更新配送员的状态 为正在配送（正在配送的，不能继续接受新单）
		     * 更新courier表的delivery_status字段值(0表示 未配送；1表示配送中)
		     * 配送完毕，修改delivery_status为1
		     * */
//		    if(StringUtils.isNotBlank(courierId)){
//		    	userDao.updateCourierDeliveryStatus(courierId, "1");
//		    }else{
//		    	/*说明该订单没有配送员，进入未分配表 unallocate_waybill table中*/
//		    	
//		    	new AsynSaveUnallocateWaybill(waybillEntity.getId(), cityCode, x, y, waybillEntity.getThird_platform_id()).run();
//		    }
		    
		   
		    
        }catch(Exception e){
        	log.error(e.getMessage());
        	throw new BaseSupportException(e);
        }
	
        
        /*异步自动分配订单*/
        new AsynAllocateWaybill(x, y, psList, cityCode, courierId, waybillEntity.getThird_platform_id()).run();;
        
	}
	
	
	/**
	 * 异步写入未自动分配的订单
	 * @author houshipeng
	 *
	 */
	class AsynAllocateWaybill extends Thread{
		String sp_x;
		String sp_y;
		List<String> list;
		String city_code;
		String courierId;
		String delivery_eid_id;
		public AsynAllocateWaybill(String sp_x,String sp_y,List<String> list,String city_code,String courierId,String delivery_eid_id){
			this.sp_x = sp_x;
			this.sp_y = sp_y;
			this.list = list;
			this.city_code = city_code;
			this.courierId = courierId;
			this.delivery_eid_id = delivery_eid_id;
		}
		
		public void run(){
			try{
				/*如果指定了配送员，无需自动分配，直接把所有的订单分配给配送员即可*/
				if(StringUtils.isNotBlank(courierId)){
					/*在controller已经检查了手机号是否存在，再次无需检查*/

					/*订单批量分配给指定的配送员*/
					waybillDao.batchAllocateWaybill2Courier(courierId,delivery_eid_id, "0", list);
					
					/*修改配送员的配送状态(如果当前状态是配送中，则不需要修改)*/
					userDao.updateCourierDeliveryStatus(courierId, "1");
					
				}else{
					/*自动分配订单*/
					
					/*根据citycode检索 在本城市配送的团队(公司)*/
					List<EnterpriseCityEntity> psEidList = getEnterpriseAreaByCityInfo(null, null, city_code, null);
					/*该区域无可用的配送公司*/
					if(psEidList==null||psEidList.size()==0){
						for(String wbid : list){
							WayBillEntity wb = new WayBillEntity();
							wb.setId(wbid);
							wb.setSys_remarks("配送未覆盖本区域,建议您取消订单");
							waybillDao.updateWaybillById(wb);
						}
						
						/*结束该线程*/
						return;
					}
					
					/*根据可用的配送公司列表，检索这些配送公司下离当前是商家最近 空闲的配送员*/
					/*获取最近的配送员*/
					
					/*解析该城市可用的配送公司*/
					StringBuilder sb = new StringBuilder("");
			        for(EnterpriseCityEntity ec : psEidList){
			        	String eid = ec.getEnterprise_id();
			        	sb.append(eid+",");
			        }

			        String eids = sb.substring(0, sb.length()-1);
			        
			        /*核心算法，查找最合适的配送员(考虑使用负载均衡算法)*/
			        String courierIdAndEid = queryNearCourier(eids, sp_x, sp_y);
			        
			        /*判断是否找到了最合适的配送员*/
			        if(StringUtils.isBlank(courierIdAndEid)){
			        	/*没有找到合适的配送员,订单写入到待分配表中*/
			        	String eid = getDeliveryEnt(psEidList);
			        	for(String wbid : list){
			        		waybillDao.saveUnallocateWaybill(wbid, city_code, sp_x, sp_y, eid);
			        	}
			        }else{
						/*格式：配送员编号^隶属企业编号*/
						String cs[] = courierIdAndEid.split("\\^");
						waybillDao.batchAllocateWaybill2Courier(cs[0],cs[1], "0", list);
						
						/*更改配送员 配送状态*/
						userDao.updateCourierDeliveryStatus(cs[0], "1");
			        }
				}
			}catch(Exception e){
				log.error("异步写入未分配的订单异常: "+e);
			}
		}
	}
	
	/**
	 * 订单自动分配过程，获取指定城市可用的配送公司
	 * @param entList
	 * @return
	 */
	String getDeliveryEnt(List<EnterpriseCityEntity> entList){
		
		/*检索支持手工干预订单的配送公司*/
		List<String> psEnt = new ArrayList<String>();
		
//		StringBuilder sb = new StringBuilder("");
		for(EnterpriseCityEntity ec : entList){
			String manual = ec.getManual();
			if(StringUtils.isNotBlank(manual)){
				if(manual.equals("0")){
					psEnt.add(ec.getEnterprise_id());
				}
			}
			//sb.append(ec.getEnterprise_id()+",");
		}
		
		/*该地区的配送公司*/
//		String eids = sb.substring(0, sb.length()-1);
		if(psEnt==null || psEnt.size()==0){
			return null;
		}
		
		/*随机返回一个配送公司*/
		int index = 0;
		if(psEnt.size()>1){
			index = new Random().nextInt(psEnt.size());
		}
		return psEnt.get(index);
	}
	
	/**
	 * 查询周边3公里范围内的空闲配送员
	 * @param eids
	 * @param x
	 * @param y
	 * @return
	 */
	private String queryNearCourier(String eids,String x,String y){
		List<String> courierList = new ArrayList<String>();
        /*查询商家周边(默认3公里)范围内的配送员，如果是多个商家，则在比较多个商家中那个离的最近(后期考虑增加权重值，用于做订单的优先分配)*/
		
//		boolean redisIsWork = redisService.redisIsWork();
		
        log.entry("courierService.queryNearCourier",eids,x,y);
        courierList = courierService.queryNearCourier(eids, x, y);
        
        log.entry("数据库中检索配送员结果: ",courierList);
        if(courierList==null||courierList.size()==0){
			return null;
        }
        
        /*获取配送员(现在是获取一个配送员，之后需要改为：如果有多个配送公司，从每个公司里面找最近的配送员，每个最近的再一次排序，找更近的)*/
        return String.valueOf(courierList.get(0));
        
        /*如果商家周边3公里范围内没有配送员，则提示 商家，周边暂时没有配送员，请稍后再提交(下一版改为 用户可以提交，提交后的订单，定时扫描是否有 可分配的配送员)，
         * 如果超过等待时间，自动取消(或者提醒商家取消订单)
         * 或者在地图上显示周边的配送员，如果地图上没有可用的配送员，需要在地图上提醒
         * */
		
		/*返回的结果为快递员id，如果返回为null，表示周边没有配送员*/
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
