package com.ksb.openapi.service.impl;

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

import autonavi.online.framework.property.PropertiesConfigUtil;
import autonavi.online.framework.sharding.dao.DaoHelper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ksb.openapi.dao.EnterpriseDao;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.EnterpriseCityEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.service.WaybillService;
import com.ksb.openapi.util.AddressUtils;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.SystemConst;

import redis.clients.jedis.GeoParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
public class WaybillServiceImpl implements WaybillService{

	private Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	WaybillDao waybillDao;
	
	@Autowired
	UserDao userDao;

	@Autowired
	EnterpriseDao enterpriseDao;
	
	@Autowired
	private JedisPool jedisPool = null;
	
	@Autowired
	WaybillGEOService waybillGEOService = null;
	
	@Autowired
	ShansongService shansongService = null;
	
	@Autowired
	DaDaService daDaService = null;
	
	/**
	 * 基于快送宝系统 web页面快送创建运单
	 * @param map
	 * @return
	 */
	@Override
	public boolean addWaybillByWeb(Map<String, String> map){
		
		/*入参 参数判断*/
		if(map==null||map.size()<1){
			throw new BaseSupportException("未提供任何参数");
		}
		
		
		/*-------------------支付信息判断-------------------------------*/
		
		/*支付类型*/
		String payType = map.get("pay_type");
		if(StringUtils.isBlank(payType)){
			throw new BaseSupportException("参数:支付类型为空");
		}
		
		/*是否需要垫付*/
		String isPrepay = map.get("is_prepay");
		if(StringUtils.isBlank(isPrepay)){
			throw new BaseSupportException("参数:是否需要垫付为空");
		}
		
		String payShipperFee = map.get("pay_shipper_fee");
		if(StringUtils.isBlank(payShipperFee)){
			payShipperFee = "0.0";
		}
		String fetchBuyerFee = map.get("fetch_buyer_fee");
		if(StringUtils.isBlank(fetchBuyerFee)){
			fetchBuyerFee = "0.0";
		}
		
		
		/*-------------------买家参数验证------------------------------*/
		
		/*买家用户名验证*/
		String buyerName = map.get("buyer_name");
		if(StringUtils.isBlank(buyerName)){
			throw new BaseSupportException("参数:收货人姓名为空");
		}
		
		/*买家电话验证*/
		String buyerPhone = map.get("buyer_phone");
		if(StringUtils.isBlank(buyerPhone)){
			throw new BaseSupportException("参数:收货人电话为空");
		}
		
		/*买家地址验证*/
		String buyerAddress = map.get("buyer_address");
		if(StringUtils.isBlank(buyerAddress)){
			throw new BaseSupportException("参数:收货人地址为空");
		}
		/*用百度地图验证买家地址是否存在*/
		ResultEntity rsEntity = null;
		try{
			rsEntity = AddressUtils.validateAddressByBDMap(buyerAddress,null);
		}catch(BaseSupportException e){
			throw e;
		}
		if(!rsEntity.isSuccess()){
			throw new BaseSupportException("参数:收货人地址不正确");
		}
		
		/*判断买家是否存在(如果存在返回用户ID；如果不存在先创建一个用户，然后返回该用户ID)*/
		Long buyerId = null;
		try {
			buyerId = (Long) userDao.queryBuyerIDByNamePhone(buyerName,buyerPhone);
			if (buyerId == null) {
				/* 说明买家不存在 */
				Map<String, String> buyerMap = new HashMap<String, String>();
				buyerMap.put("name", buyerName);
				buyerMap.put("real_name", buyerName);
				buyerMap.put("phone", buyerPhone);

				buyerMap.put("address", buyerAddress);
				
				try{
					String xy = rsEntity.getObj().toString();
					String[] xys = xy.split(";");
					buyerMap.put("x", xys[0]);
					buyerMap.put("y", xys[1]);
				}catch(Exception e){
					log.error("["+buyerAddress+"]地址正确，但是无法提取经纬度信息");
				}
				userDao.createBuyer(buyerMap);
				buyerId = DaoHelper.getPrimaryKey();
			}
		} catch (Exception e) {
			throw new BaseSupportException(e);
		}
		
		/*------------------判断商家信息----------------------*/
		
		/*买家用户名验证*/
		String shipperName = map.get("shipper_name");
		if(StringUtils.isBlank(shipperName)){
			throw new BaseSupportException("参数:发货人姓名为空");
		}
		
		/*买家电话验证*/
		String shipperPhone = map.get("shipper_phone");
		if(StringUtils.isBlank(shipperPhone)){
			throw new BaseSupportException("参数:发货人电话为空");
		}
		
		/*买家地址验证*/
		String shipperAddress = map.get("shipper_address");
		if(StringUtils.isBlank(shipperAddress)){
			throw new BaseSupportException("参数:发货人地址为空");
		}
		/*用百度地图验证卖家地址是否存在*/
		rsEntity = null;
		try{
			rsEntity = AddressUtils.validateAddressByBDMap(shipperAddress,null);
		}catch(BaseSupportException e){
			throw e;
		}
		if(!rsEntity.isSuccess()){
			throw new BaseSupportException("参数:发货人地址不正确");
		}
		
		/*判断买家是否存在(如果存在返回用户ID；如果不存在先创建一个用户，然后返回该用户ID)*/
		Long shipperId = null;
		try {
			shipperId = (Long) enterpriseDao.queryShipperByNamePhone(shipperName, shipperPhone);
			
			if (shipperId == null) {
				/* 说明买家不存在 */
				Map<String, String> shipperMap = new HashMap<String, String>();
				shipperMap.put("name", shipperName);
				shipperMap.put("phone", shipperPhone);
				shipperMap.put("address", shipperAddress);
				
				/*记录商家的的经纬度信息*/
				try{
					String xy = rsEntity.getObj().toString();
					String[] xys = xy.split(";");
				    shipperMap.put("x", xys[0]);
				    shipperMap.put("y", xys[1]);
				}catch(Exception e){
					log.error("["+shipperAddress+"]地址正确，但是无法提取经纬度信息");
				}
				enterpriseDao.createShipper(shipperMap);
				shipperId = DaoHelper.getPrimaryKey();
			}
		} catch (Exception e) {
			throw new BaseSupportException(e);
		}
		
		/*-----------------------------拼装运单数据-------------------------------------*/
		
		Map<String, String> waybillMap = new HashMap<String, String>();
		waybillMap.put("buyer_id", String.valueOf(buyerId));
		waybillMap.put("shippers_id", String.valueOf(shipperId));
		waybillMap.put("pay_type", payType);
		waybillMap.put("is_prepay", isPrepay);
		waybillMap.put("third_platform_id", "0");
		waybillMap.put("pay_shipper_fee", payShipperFee);
		waybillMap.put("fetch_buyer_fee", fetchBuyerFee);
		try {
			waybillMap.put("id", "3km-"+DaoHelper.createPrimaryKey());
		} catch (Exception e1) {
			throw new BaseSupportException(e1);
		}
		try {
			waybillDao.createKSBWayBill(waybillMap);
		} catch (Exception e) {
			throw new BaseSupportException(e);
		}
		
		return true;
	}
	
	/**
	 * 基于openapi(为第三方提供的 订单导入接口)
	 * @param map
	 * @return
	 */
	public boolean addWaybillByOpenApi(Map<String, String> map){
		
		/*入参 参数判断*/
		if(map==null||map.size()<1){
			throw new BaseSupportException("未提供任何参数");
		}
		
		/*---------------------运单基本信息----------------------------*/
		/*是否预约取件
		 * 0、立即取件  1、预约取件
		 * */
		String bookingFetch = map.get("booking_fetch");
		if(StringUtils.isBlank(bookingFetch)){
			throw new BaseSupportException("参数booking_fetch:取货类型未指定(立即去/预约)");
		}
		/*预约取件时间 格式yyy-MM-dd HH:ss(只有booking_fetch为1的时候才验证 booking_fetch_time字段有效性)*/
		String bookingFetchTime = map.get("booking_fetch_time");
		if(bookingFetch.equals("1")){
			if(StringUtils.isBlank(bookingFetchTime)){
				throw new BaseSupportException("参数booking_fetch_time:未指定预约取货时间");
			}
		}
		/*运单所在的省*/
		String provinceName = map.get("province_name");
//		if(StringUtils.isBlank(provinceName)){
//			throw new BaseSupportException("参数province_name 运单所在的省信息为空");
//		}
		/*运单所在的市*/
		String cityCode = map.get("city_code");
		if(StringUtils.isBlank(cityCode)){
			throw new BaseSupportException("参数city_code 运单所在的市信息为空");
		}
		
		/*-------------------支付信息判断-------------------------------*/
		
		/*支付类型*/
		String payType = map.get("pay_type");
//		if(StringUtils.isBlank(payType)){
//			throw new BaseSupportException("参数:支付类型为空");
//		}
		
		/*是否需要垫付*/
		String isPrepay = map.get("is_prepay");
//		if(StringUtils.isBlank(isPrepay)){
//			throw new BaseSupportException("参数:是否需要垫付为空");
//		}
		
		String payShipperFee = map.get("pay_shipper_fee");
		String fetchBuyerFee = map.get("fetch_buyer_fee");
		if(StringUtils.isBlank(payShipperFee)){
			payShipperFee = "0.0";
		}
		if(StringUtils.isBlank(fetchBuyerFee)){
			fetchBuyerFee = "0.0";
		}
		
		/*-------------------判断货物信息-----------------------------*/
		
		/*货物类型*/
//		String cargoType = map.get("cargo_type");
//		if(StringUtils.isBlank(cargoType)){
//			throw new BaseSupportException("参数:cartgo_type货物类型为空");
//		}
		String cargoName = map.get("cargo_name");
		if(StringUtils.isBlank(cargoName)){
			throw new BaseSupportException("参数:cargoName货物名称为空");
		}		
		/*货物重量*/
		String cargoWeight = map.get("cargo_weight");
		if(StringUtils.isBlank(cargoWeight)){
			throw new BaseSupportException("参数:cartgo_weight货物重量为空");
		}
		/*货物价格*/
		String cargoPrice = map.get("cargo_price");
		if(StringUtils.isBlank(cargoPrice)){
			throw new BaseSupportException("参数:cartgo_price货物价格为空");
		}
		/*货物数量*/
		String cargoNum = map.get("cargo_num");
//		if(StringUtils.isBlank(cargoNum)){
//			throw new BaseSupportException("参数:cartgo_num货物数量为空");
//		}
		
		/*------配送平台(以后默认走快送宝配送平台，除非硬性指定配送平台(前期仅支持达达和闪送))--------*/
		
		String thirdPlatformId = map.get("third_platform_id");
		if(StringUtils.isBlank(thirdPlatformId)){
			//throw new BaseSupportException("参数:thirdPlatformId 未指定配送平台");
			/*默认走快送宝配送平台*/
			thirdPlatformId = "0";
		}
		
		/*-------------------买家参数验证------------------------------*/
		
		/*支持多个买家，信息格式如下
		 * 
		 * [
		 * {name:kevi,phone:134***,x:116.39087,y:39.3459,address:北京市海淀区北四环西路68号左岸公社706}，
		 * {name:qkzhao,phone:132***,x:116.34087,y:39.3059,address:北京市朝阳区SOHO8层806}
		 * ]
		 * 
		 * */
		/*买家用户名验证*/
		String buyersListInfo = map.get("buyers_list");
		if(StringUtils.isBlank(buyersListInfo)){
			throw new BaseSupportException("参数:收货人列表为空");
		}
		
		List<BuyerEntity> buyersList = null;
		try{
			buyersList = JSON.parseArray(buyersListInfo, BuyerEntity.class);
		}catch(Exception e){
			/*收件人列表 json格式异常*/
			throw new BaseSupportException(e);
		}
		
		if(buyersList==null||buyersList.size()==0){
			/*收件人列表 为空*/
			throw new BaseSupportException("无法获取收货人列表信息");
		}
		
		/*批量验证收货人地址是否合法(之后考虑 在验证失败后返回验证失败的地址;本期提供地址验证接口，提交订单前可以使用地址验证接口)*/
		ResultEntity validateEntity = AddressUtils.batchValidateBuyerAddress(buyersList);
		if(!validateEntity.isSuccess()){
			/*收件人列表 为空*/
			throw new BaseSupportException("收货人列表中 存在非法的收货地址");
		}
		
		/*批量地址验证中，获取地址对应的经纬度信息*/
		Map<String, String> addressXYMap = (Map<String, String>)validateEntity.getObj();
		
		/*批量判断买家是否存在(如果存在返回用户ID；如果不存在先创建一个用户，然后返回该用户ID)*/
		for (BuyerEntity entity : buyersList) {

			Long buyerId = null;
			try {
				buyerId = (Long) userDao.queryBuyerIDByNamePhone(entity.getName(),entity.getPhone());
				if (buyerId == null) {
					/* 说明买家不存在 */
					Map<String, String> buyerMap = new HashMap<String, String>();
					buyerMap.put("name", entity.getName());
					buyerMap.put("real_name", entity.getName());
					buyerMap.put("phone", entity.getPhone());

					buyerMap.put("address", entity.getAddress());

					try {
						String xy = addressXYMap.get(entity.getPhone()).toString();
						String[] xys = xy.split(";");
						buyerMap.put("x", xys[0]);
						buyerMap.put("y", xys[1]);
					} catch (Exception e) {
						log.error("[" + entity.getAddress() + "]地址正确，但是无法提取经纬度信息");
					}
					userDao.createBuyer(buyerMap);
					buyerId = DaoHelper.getPrimaryKey();
				}
				
				/*把查询到得/新创建的用户ID，回执到entity对象中，用于批量运单入库*/
				entity.setId(buyerId.toString());
				
				/*每个客户是一个独立的运单*/
//				entity.setWabill_id("3km-"+DaoHelper.createPrimaryKey().toString());
				entity.setWbid("3km-"+DaoHelper.createPrimaryKey().toString());
			} catch (Exception e) {
				throw new BaseSupportException(e);
			}
		}
		
		/*------------------判断商家信息----------------------*/
		
		/*发货人验证*/
		String shipperName = map.get("shipper_name");
		if(StringUtils.isBlank(shipperName)){
			throw new BaseSupportException("参数:发货人姓名为空");
		}
		
		/*发货人电话验证(后期考虑使用正则判断电话号码是否符合规则)*/
		String shipperPhone = map.get("shipper_phone");
		if(StringUtils.isBlank(shipperPhone)){
			throw new BaseSupportException("参数:发货人电话为空");
		}
		
		/*发货人地址验证*/
		String shipperAddress = map.get("shipper_address");
		if(StringUtils.isBlank(shipperAddress)){
			throw new BaseSupportException("参数:发货人地址为空");
		}
		
		/*用百度地图验证卖家地址是否存在(需要优化，以后需要调整为：如果商家已经存在，判断库中得地址和当前传入的地址是否一样 这样可以解决商家搬家的问题)*/
		ResultEntity rsEntity = new ResultEntity();
		
		/*判断前段是否已经获取了经纬度*/
		String shipperAddressX = map.get("shipper_address_x");
		String shipperAddressY = map.get("shipper_address_y");
		if(StringUtils.isNotBlank(shipperAddressX)&&StringUtils.isNotBlank(shipperAddressY)){
			rsEntity.setSuccess(true);
			rsEntity.setObj(shipperAddressX+";"+shipperAddressY);
		}else{
			try{
				rsEntity = AddressUtils.validateAddressByBDMap(shipperAddress,null);
			}catch(BaseSupportException e){
				throw e;
			}
		}

		if(!rsEntity.isSuccess()){
			throw new BaseSupportException("参数:发货人地址不正确");
		}
		
		/*判断买家是否存在(如果存在返回用户ID；如果不存在先创建一个用户，然后返回该用户ID)*/
		Long shipperId = null;
		
		/*两个冗余字段，在运单提交 自动分单的时候，需要使用商家位置为中心点做配送优化*/
		ShipperEntity shipperEntity = new ShipperEntity();
		try {
			
			/*后期queryShipperByNamePhone方法改为返回值为 shipperEntity对象*/
			shipperId = (Long) enterpriseDao.queryShipperByNamePhone(shipperName, shipperPhone);
			
			if (shipperId == null) {
				/* 说明买家不存在 */
				Map<String, String> shipperMap = new HashMap<String, String>();
				shipperMap.put("name", shipperName);
				shipperMap.put("phone", shipperPhone);
				shipperMap.put("address", shipperAddress);
				
				/*记录商家的的经纬度信息*/
				try{
					String xy = rsEntity.getObj().toString();
					String[] xys = xy.split(";");
				    shipperMap.put("x", xys[0]);
				    shipperMap.put("y", xys[1]);
				}catch(Exception e){
					log.error("["+shipperAddress+"]地址正确，但是无法提取经纬度信息");
				}
				enterpriseDao.createShipper(shipperMap);
				shipperId = DaoHelper.getPrimaryKey();
				
				shipperEntity.setId(String.valueOf(shipperId));
				shipperEntity.setAddress(shipperAddress);
				shipperEntity.setAddress_x(shipperMap.get("x"));
				shipperEntity.setAddress_y(shipperMap.get("y"));
				shipperEntity.setName(shipperName);
				shipperEntity.setPhone(shipperPhone);
			}
		} catch (Exception e) {
			throw new BaseSupportException(e);
		}
		
		
		/*-----------------------------拼装运单数据-------------------------------------*/
		
		/*拼装运单数据*/
		WayBillEntity waybillEntity = new WayBillEntity();
		
		/*运单所在的省市信息*/
		waybillEntity.setProvince_name(provinceName);
//		waybillEntity.setCity_name(cityName);
		waybillEntity.setCity_code(cityCode);
		/*商家ID*/
		waybillEntity.setShippers_id(String.valueOf(shipperId));
		/*支付类型*/
		waybillEntity.setPay_type(payType);
		/*是否需要垫付*/
		waybillEntity.setIs_prepay(isPrepay);
		/*配送平台*/
		waybillEntity.setThird_platform_id(thirdPlatformId);
		/*货物类型*/
		waybillEntity.setCargo_type(cargoWeight);
		/*货物重量*/
		waybillEntity.setCargo_weight(cargoWeight);
		/*货物价格*/
		waybillEntity.setCargo_price(cargoPrice);
		/*货物数量*/
		waybillEntity.setCargo_num(cargoNum);
		/*导入批次 正式版:批次头为B； 测试版:数据批次头位TB*/
		try {
			waybillEntity.setImport_batch("B-"+DaoHelper.createPrimaryKey());
		} catch (Exception e1) {
			throw new BaseSupportException(e1);
		}
		/*是否预约取件*/
		waybillEntity.setBooking_fetch(bookingFetch);
		
		if(StringUtils.isNotBlank(bookingFetch)&&bookingFetch.equals("1")){
			/*预约取件时间*/
			Long bookingTime = new Date().getTime();
			try{
			   bookingTime = DateUtil.convertStringToDateHour(bookingFetchTime).getTime();
			   waybillEntity.setBooking_fetch_time(bookingTime);
			}catch(Exception e){}
		}
		
		/*向商家支付的费用*/
		waybillEntity.setPay_shipper_fee(payShipperFee);
		/*向买家收取的费用*/
		waybillEntity.setFetch_buyer_fee(fetchBuyerFee);
		/*运单提交时间*/
		waybillEntity.setCreate_time(new Date().getTime());
		waybillEntity.setWaybill_status("2");
		waybillEntity.setPayment_status("0");
		waybillEntity.setWaybill_type("2");
		
		/*根据城市名，获取负责该城市的配送企业列表*/
		List<EnterpriseCityEntity> entList = (List<EnterpriseCityEntity>)enterpriseDao.queryEnterpriseByCityInfo(null, null, cityCode, null);
		if(entList==null || entList.size()==0){
		   throw new BaseSupportException("该城市无配送团队");	
		}
		
		try {
			waybillDao.batchCreateKSBWayBillByBuyerAddress(waybillEntity, buyersList);
		} catch (Exception e) {
			throw new BaseSupportException(e);
		}
		
		/*异步把运单 数据放入到redis，自动优化分单*/
		WaybillAllocationStrategy thread2Redis = new WaybillAllocationStrategy(shipperEntity,waybillEntity, buyersList,entList);
		thread2Redis.run();
		
		return true;
	}
	
	/**
	 * 临时自动化分单策略
	 * 该策略 仅适用快送宝还没有自己配送团队的时候，使用第三方配送平台
	 * @author houshipeng
	 *
	 */
	public class WaybillAllocationStrategy extends Thread{
		WayBillEntity waybillEntity = null;
		List<BuyerEntity> buyerList = null;
		ShipperEntity shipperEntity = null;
		List<EnterpriseCityEntity> entList=null;
		public WaybillAllocationStrategy(ShipperEntity shipperEntity,WayBillEntity waybillEntity,List<BuyerEntity> buyerList,List<EnterpriseCityEntity> entList){
			this.waybillEntity = waybillEntity;
			this.buyerList = buyerList;
			this.shipperEntity = shipperEntity;
			this.entList = entList;
		}
		
		public void run(){
			
			/*获取客户选择的配送平台*/
			int psPlatform = Integer.parseInt(waybillEntity.getThird_platform_id());
			switch (psPlatform) {
			case 0:
				/*系统自动分配配送公司(通过webapi提交的订单，订单默认分配。之后分配策略需要修改)*/
				String eid = entList.get(0).getEnterprise_id();
				ksbStrategy(waybillEntity.getCity_code(),shipperEntity.getAddress_x(),shipperEntity.getAddress_y(),buyerList, eid);
				break;
			case 1:	
                /*达达配送*/
				List<String> waybillIdList = new ArrayList<String>();
				for(BuyerEntity be : buyerList){
					waybillIdList.add(be.getWbid());
				}
				/*批量把运单推送到达达配送平台*/
				dadaStrategy(waybillEntity,shipperEntity,buyerList);
				break;
			case 2:
				/*闪送配送*/
				ishansongStrategy(shipperEntity, waybillEntity, buyerList);
				break;
			default:
				/*快送宝配送*/
				log.warn("暂不支持");
				break;
			}
		}
	}
	
	private void ksbStrategy(String cityCode,String sp_x,String sp_y,List<BuyerEntity> buyerList,String eid){
		
		/*批量把快送宝运单提交到 达达平台*/
		
		for(BuyerEntity be : buyerList){
			String ksbId = be.getWbid();
			waybillDao.allocationWayBill2ThirdParty(ksbId, eid, "0");
			
			/*同时把订单放入到 待分配表中*/
			waybillDao.saveUnallocateWaybill(ksbId, cityCode, sp_x, sp_y, eid);
		}
	}
	
	/**
	 * 达达分配策略: 快送宝运单和达达运单一对一
	 *             不做任何合单优化每个快送宝运单提交一次达达openapi创建订单接口
	 * @param waybillIdList
	 */
	private void dadaStrategy(WayBillEntity waybillEntity,ShipperEntity shipperEntity,List<BuyerEntity> buyerList){
		
		/*批量把快送宝运单提交到 达达平台*/
		Map<String, String> mapLink = daDaService.batchSubmitOrder(waybillEntity, shipperEntity, buyerList);
		
		Iterator<Entry<String, String>> it = mapLink.entrySet().iterator();
		while(it.hasNext()){
			
			Entry<String, String> entry = it.next();
			String ksbId = entry.getKey();
			String dadaId = entry.getValue();
			
			waybillDao.allocationWayBill2ThirdParty(ksbId, "1", dadaId);
			
		}
		
	}
	

	/**
	 * 闪送分配策略: 本地优化合单 按商家到买家的距离每三个运单为一组 优化组合
	 * @param shipperX
	 * @param shipperY
	 *        商家经纬度，需要以此为中心点 优化配送
	 * 
	 * @param waybillEntity
	 * @param buyerList
	 */
	private void ishansongStrategy(ShipperEntity shipperEntity,WayBillEntity waybillEntity,List<BuyerEntity> buyerList){
		
		/*导入批次号,redis中存储的主键*/
		String importBatch = waybillEntity.getImport_batch();
		
		/*数据转为map格式 k:运单ID;v:买家坐标*/
		Map<String, String> geoMap = new HashMap<String, String>();
		List<String> waybillIdList = new ArrayList<String>();
		for(BuyerEntity be : buyerList){
			geoMap.put(be.getWbid(), be.getAddress_x()+";"+be.getAddress_y());
			waybillIdList.add(be.getWbid());
		}
		//buyerList.clear();
		
		/*订单的空间数据入库，便于之后 以商家为中心点 寻找最近的买家*/
		waybillGEOService.oneShipperMoreBuyerGEO2Redis(importBatch, geoMap);
		
		/*以商家的地址为中心点，优化分单(返回结果嵌套的每个list 就是一个合并分组)*/
		List<List<String>> rsList = waybillGEOService.optimizeGroupWaybill(Double.parseDouble(shipperEntity.getAddress_x()), Double.parseDouble(shipperEntity.getAddress_y()), importBatch, waybillIdList);
		
		/*向闪送提交订单*/
		Map<String, List<String>> shansongLinkMap = shansongService.batchSubmitShansongOrder(shipperEntity,waybillEntity, buyerList,rsList);
		
		/*把闪送订单id和快送宝运单id 做对应(更新快送宝运单库中 第三方配送平台ID字段)*/
		Iterator<Entry<String, List<String>>> it = shansongLinkMap.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, List<String>> entry = it.next();
			waybillDao.batchAllocationWayBill2ThirdParty(entry.getValue(), "2", entry.getKey());
		}
		
		/*清理内存*/
		buyerList.clear();
		rsList.clear();
	}
	
	
	@Override
	public boolean allocationWaybill2Courier(String waybillId, String courierId) {
		// TODO Auto-generated method stub
		if(StringUtils.isBlank(waybillId) || StringUtils.isBlank(courierId)){
			throw new BaseSupportException("必须得参数为空，无法执行运单分配动作");
		}
		try{
			waybillDao.allocationWayBill2Courier4KSB(waybillId, courierId);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		return true;  
	}
	
	@Override
	public Map<String, Object> searchShipperOrderInfo(Long id, String orderId,String status,
			String buyerName, String buyerPhone) {
		// TODO Auto-generated method stub
		
		/*方法入参*/
		log.entry(id,orderId,status,buyerName,buyerPhone);
		
		/*基本的参数判断*/
		if(id==null){
			log.error("必须得参数 商家ID 为空");
			return null;
		}
		
		List<Map<String, Object>> list = null;
		
		try {
			list = (List<Map<String, Object>>) waybillDao.searchWaybillInfo(id.toString(), orderId, status, buyerPhone, buyerName);
		} catch (Exception e) {
			log.error("查询商家所有的运单数据异常",e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		rsMap.put("1", list);
		
		/*预留分页数据*/
		//rsMap.put("2", DaoHelper.getCount());
		return log.exit(rsMap);
	}
	
	
	@Override
	public Map<String, Object> searchWaybillInfo(Map<String, String> pm, int skip, int size) {
		// TODO Auto-generated method stub
		
		if(pm==null){
			pm = new HashMap<String, String>();
		}
		
		List<Map<String, Object>> rsList = null;
		try{
			rsList = (List<Map<String, Object>>) waybillDao.searchWaybillInfo(pm, skip, size);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		rsMap.put("1", converRsListID(rsList));
		rsMap.put("2", DaoHelper.getCount());
		
		return rsMap;
	}
	
	private List<Map<String, Object>> converRsListID(List<Map<String, Object>> list){
		
		if(list==null||list.size()==0){
			return list;
		}
		List<Map<String, Object>> rsList = new ArrayList<Map<String,Object>>();
		
		for(Map<String, Object> m : list){
			Object objId = m.get("id");
			m.put("id", objId.toString());
			rsList.add(m);
		}
		return rsList;
	}

	
	
	
	
	
	
	
}
