package com.ksb.openapi.mobile.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import redis.clients.jedis.JedisPool;
import autonavi.online.framework.property.PropertiesConfigUtil;
import autonavi.online.framework.sharding.dao.DaoHelper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.em.WaybillType;
import com.ksb.openapi.entity.BillEntity;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.O2oWaybillService;
import com.ksb.openapi.util.AddressUtils;
import com.ksb.openapi.util.CoorUtils;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.JedisUtil;
import com.ksb.openapi.util.SystemConst;

@Service
public class O2oWaybillServiceImpl implements O2oWaybillService {

	@Autowired
	WaybillDao waybillDao;
	
	@Autowired
	UserDao userDao;
	
	private String REDIS_KEY_PREFIX = "O2O";
	private Logger log = LogManager.getLogger(getClass());
	
	@Override
	public Map<String, Object> queryWaybillByCourier(String courierId,
			String waybillId, String waybillStatus, int skip, int size) {
		
		log.entry(courierId,waybillId,waybillStatus,skip,size);
		/*必须得参数快递员ID不能为空*/
		if(StringUtils.isBlank(courierId)){
			log.error("参数：配送员编号[courierId] 为空");
			throw new BaseSupportException("参数：配送员编号[courierId] 为空");
		}
		
		List<Map<String, String>> rsList = null;
		try{
			log.entry("请求waybillDao.queryCourierTodayO2OWaybillList: 参数：",courierId, waybillId, waybillStatus, WaybillType.O2O.getName(), skip, size);
			rsList = (List<Map<String, String>>)waybillDao.queryCourierTodayO2OWaybillList(courierId, waybillId, waybillStatus, WaybillType.O2O.getName(), skip, size);
		    log.debug("dao返回结果"+rsList);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		
		/*返回的数据中增加距离参数*/
		rsMap.put("1", handleCourierWaybill(rsList));
		rsMap.put("2", DaoHelper.getCount());
		
		return log.exit(rsMap);
	}

	
	private List<Map<String, String>> handleCourierWaybill(List<Map<String, String>> srcList){
		
		String picHost = "http://resource.3gongli.com/pic";
		try {
			picHost = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.PIC_URL);
		} catch (Exception e) {}
		/*拼装图片地址*/
		for(Map<String, String> map : srcList){
			String imgName = map.get("img_name");
			String imgPath = getImgPathByImageId(imgName);
			if(StringUtils.isNotBlank(imgPath)){
				map.put("pic_url",picHost+imgPath );
			}
		}
		
		return srcList;
	}
	
	public String getImgPathByImageId(String imageId) {
		
		if (StringUtils.isBlank(imageId)) {
			return "";
		}
		String imgName = StringUtils.replace(imageId, "_", File.separator);
		String resultPath = imgName.substring(0, imgName.lastIndexOf(File.separator));
		StringBuilder imgPath = new StringBuilder();
		imgPath.append(File.separator).append(resultPath).append(File.separator).append(imageId).append(".jpg");
		return imgPath.toString();
	}
	
	public static void main(String[] args) {
		
		O2oWaybillServiceImpl o = new O2oWaybillServiceImpl();
		o.getImgPathByImageId("201509_10_0_uCDpb64192273194352640043041");
	}
	
	/**
	 * O2O运单查询结果，增加距离属性
	 * @param srcList
	 * @return
	 */
	private List<Map<String, String>> addDistancePro(List<Map<String, String>> srcList){
		
		for(Map<String, String> map : srcList){
			
			String startX = map.get("shipper_x");
			String startY = map.get("shipper_y");
			if(StringUtils.isBlank(startX)||StringUtils.isBlank(startY)){
				map.put("distance", "未知");
				continue;
			}
			
			String endX = map.get("buyer_x");
			String endY = map.get("buyer_y");
			if(StringUtils.isBlank(endX)||StringUtils.isBlank(endY)){
				map.put("distance", "未知");
				continue;
			}
			
			double distance = CoorUtils.distance(Double.parseDouble(startX), Double.parseDouble(startY), Double.parseDouble(endX), Double.parseDouble(endY));
			java.text.DecimalFormat   df   =new   java.text.DecimalFormat("######0.00"); 
			map.put("distance", df.format(distance/1000)+"km");
			map.put("shipper_address", map.get("shipper_address")+"--"+map.get("shipper_address_detail"));
		}
		return srcList;
	}
	
	@Override
	public List<Map<String, String>> currentDayWayBillStatistic(String courierId) {
		
		log.entry(courierId);
		List<Map<String, String>> list = null;
		/*首先从redis中获取各个状态的运单数量*/
		boolean redisEnable = false;
		try {
			/*判断是否启用redis*/
			String redisEnableStr = (String) PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.REDIS_ENABLE);
			log.debug("是否启用redis: "+redisEnableStr);
			redisEnable = Boolean.parseBoolean(redisEnableStr);
			if(redisEnable){
				list = countStatusNumByRedis(courierId);
				log.debug("redis中获取的数据："+list);
				if(list!=null&&list.size()>0){
					return list;
				}
			}
		}catch(Exception e){}
		try{
			long startTime = DateUtil.getTodayStartTime();
			//long startTime = 0;
			long endTime = DateUtil.getTodayEndTime();
			log.entry("waybillDao.currentDayWaybillStatByCourier",courierId, startTime, endTime,WaybillType.O2O.getName());
		    list = (List<Map<String, String>>)waybillDao.currentDayWaybillStatByCourier(courierId, startTime, endTime,WaybillType.O2O.getName());
		    
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		return log.exit(handleCourierCountInfo(list));
	}

	/**
	 * 获取默认的统计项(防止数据库中没有某一个状态值)
	 * @return
	 */
	private Map<String, Map<String, String>> getDefaultStatMap(){
		
		Map<String, Map<String, String>> rsMap = new HashMap<String, Map<String,String>>();
		
		for(int i=-2;i<=5;i++){
			Map<String, String> tm = new HashMap<String, String>();
			tm.put("status", i+"");
			tm.put("num", "0");
			rsMap.put(i+"", tm);
		}
		
		return rsMap;
	}
	
	private List<Map<String, String>> handleCourierCountInfo(List<Map<String, String>> list){
		
		try {
			/* 所有状态码先都置为0(相当于模板) */
			Map<String, Map<String, String>> tm = getDefaultStatMap();

			Map<String, Map<String, String>> tmpMap = new HashMap<String, Map<String, String>>();
			for (Map<String, String> m : list) {
				String statusCode = String.valueOf(m.get("status"));
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
			//e.printStackTrace();
			return list;
		}
	}
	
	
	/**
	 * 从配送员ID，从rendis中抓取当前该配送员所有的运单统计数据
	 * @param courierId
	 * @return
	 */
	private List<Map<String, String>> countStatusNumByRedis(String courierId){
		
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();

		try {

			String todayStr = DateUtil.convertDateToString(DateUtil.getCurrentDate(),"yyyyMMdd");
			/*循环0-5状态码*/
			for(int i=0;i<=5;i++){
				Map<String, String> tm = new HashMap<String, String>();
				tm.put("status", i+"");
				long tn = JedisUtil.getInstance().SETS.scard(REDIS_KEY_PREFIX+"_"+courierId + "_"+i+"_"+todayStr);
				tm.put("num", tn+"");
				list.add(tm);
			}
			
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
	public void batchFetchO2OWaybill(Map<String, String> paraMap) {
		// TODO Auto-generated method stub
		log.entry(paraMap);
		/*快递员*/
		String courierId = paraMap.get("cid");
		
		/*快送宝运单ID(非电商ID)*/
		String handleListStr = paraMap.get("handle_list");
		/*解析json数据格式[{id:"",status:}...]*/
	
		List<String> waybillIdList = new ArrayList<String>();
		try{
			JSONArray dataList = JSON.parseArray(handleListStr);
			for(int i=0;i<dataList.size();i++){
				String wbId = dataList.getJSONObject(i).getString("id");
				waybillIdList.add(wbId);
			}
		}catch(Exception e){}
        
		try{
			log.entry(courierId, waybillIdList);
		    waybillDao.batchFetchO2OWaybill(courierId, waybillIdList);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
		
		/*状态修改成功，配送记录表中 增加配送记录*/
		
	}
	
	@Override
	public Map<String, String> courierPaymentStat(String cid){
		
		Map<String, String> rsMap = null;
		try{
			long startTime = DateUtil.getTodayStartTime();
			//long startTime = 0;
			long endTime = DateUtil.getTodayEndTime();
			rsMap = (Map<String, String>)waybillDao.courierPaymentStat(cid, startTime, endTime, WaybillType.O2O.getName());
			if(rsMap==null){
				rsMap = getDefaultRs();
			}else{
				double fetchFees = Double.parseDouble(String.valueOf(rsMap.get("total_buyer_fees")));
				double payFees = Double.parseDouble(String.valueOf(rsMap.get("total_shipper_fees")));
				rsMap.put("total_pay_fees", (fetchFees-payFees)+"");
			}
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		return rsMap;
	}
	
	private Map<String, String> getDefaultRs(){
		Map<String, String> map = new HashMap<String, String>();
		map.put("total_waybill", "0");
		map.put("total_shipper_fees", "0");
		map.put("total_buyer_fees", "0");
		map.put("total_pay_fees", "0");
		map.put("average_time", "0");
		map.put("total_price", "0");
		
		return map;
	}
	
	@Override
	public void updateCustomRemark(String cid, String waybillId,
			String customRemarks) {
		log.entry(cid,waybillId,customRemarks);
		
		if(StringUtils.isBlank(cid)){
			throw new BaseSupportException("配送员编号为空");
		}
		if(StringUtils.isBlank(waybillId)){
			throw new BaseSupportException("订单编号为空");
		}
		
		try{
			waybillDao.updateWaybillCustomRemarks(cid, waybillId, customRemarks);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}	
	
	@Override
	public void saveWaybillImgId(String cid,String wbid,String imgId,String pay_sp_fee,String fetch_buyer_fee){
	
		try{
			WayBillEntity entity = new WayBillEntity();
			entity.setCourier_id(cid);
			entity.setId(wbid);
			entity.setImg_name(imgId);
			entity.setPay_shipper_fee(pay_sp_fee);
			entity.setFetch_buyer_fee(fetch_buyer_fee);
			
			waybillDao.updateWaybillById(entity);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}
	
	@Override
	public void saveBuyerInfo(BuyerEntity entity,double sp_x,double sp_y,String sp_id){
		
		log.entry(entity.getName(),entity.getPhone(),entity.getCity_name(),entity.getAddress(),sp_x,sp_y,sp_id);
		
		/*判断用户是否注册过(手机号+地址 确定一个唯一用户)*/
		String phone = entity.getPhone();
		String address = entity.getAddress();
		
		BuyerEntity queryBuyer = (BuyerEntity)userDao.queryBuyerIDByAddressAddress(phone, address);
		if(queryBuyer==null){
			/*判断用户提供的地址是否有效(百度逆地理编码)*/
			ResultEntity rsEntity = null;
			try{
				rsEntity = AddressUtils.validateAddressByBDMap(address,entity.getCity_name());
			}catch(BaseSupportException e){
				throw e;
			}
			if(!rsEntity.isSuccess()){
				throw new BaseSupportException("收货人地址无法识别");
			}
			
			/*从百度你地理编码接口结果中获取地址对应的经纬度*/
			try{
				String xy = rsEntity.getObj().toString();
				String[] xys = xy.split(";");
				entity.setAddress_x(xys[0]);
				entity.setAddress_y(xys[1]);
			}catch(Exception e){
				log.error("无法提取经纬度信息");
				throw new BaseSupportException("无法提取经纬度信息");
			}
			
			/*新用户，需要入库*/
			Long buyerId = null;
			try {
				buyerId = DaoHelper.createPrimaryKey();
			} catch (Exception e1) {
				log.error("创建买家用户的时候[DaoHelper.createPrimaryKey()]异常");
				throw new BaseSupportException("系统内部异常");
			}
			entity.setId(String.valueOf(buyerId));
			try{
				userDao.createBuyer(entity);
			}catch(Exception e){
				throw new BaseSupportException(e);
			}
		}else{
			entity = queryBuyer;
		}
		
		/*计算订单配送距离*/

		double buyer_x = Double.parseDouble(entity.getAddress_x());
		double buyer_y = Double.parseDouble(entity.getAddress_y());
		
		
		double waybill_distance = CoorUtils.distance(sp_x, sp_y, buyer_x, buyer_y);
		/*单位千米 保留小数点到两位*/
		java.text.DecimalFormat df = new java.text.DecimalFormat("######0.00");
		/*计算商家到实际买家之间的直线距离*/
		String distanceStr = df.format(waybill_distance/1000);
		
		/*配送员编号*/
		String courierId = entity.getCid();
		/*订单编号*/
		String wbid = entity.getWbid();
		/*买家编号*/
		String buyerId = entity.getId();
		
		WayBillEntity wb = new WayBillEntity();
		wb.setWaybill_distance(distanceStr);
		wb.setCourier_id(courierId);
		wb.setBuyer_id(String.valueOf(buyerId));
		wb.setId(wbid);
		
		try{
			/*更新订单中买家信息、订单配送距离*/
			waybillDao.updateWaybillById(wb);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		/*异步根据配送距离计算配送费*/
		BillEntity bill = new BillEntity();
		bill.setCid(courierId);
		bill.setSp_id(sp_id);
		bill.setStatus("0");
		bill.setWaybill_id(wbid);
		bill.setWaybill_distance(distanceStr);
		bill.setBuyer_id(buyerId);
		new AsynSaveDeliveryBill(bill).run();
	}
	
	/**
	 * 异步生成配送费用清单
	 * @author houshipeng
	 *
	 */
	class AsynSaveDeliveryBill extends Thread{
		
		public BillEntity bill;
		public AsynSaveDeliveryBill(BillEntity bill){
			this.bill = bill;
		}
		
		public void run(){
			try{
				/*根据计费规则，按距离计算订单的配送费用*/
				String distance = bill.getWaybill_distance();
				
				/*根据距离计算配送费*/
				double totalAmount = CoorUtils.getAmountByDistance(Double.parseDouble(distance));
				
				/*生成账单id*/
				String billId = null;
				try{
					billId = String.valueOf(DaoHelper.createPrimaryKey());
				}catch(Exception e){}
				
				/*封装费用清单*/
				bill.setAmount(String.valueOf(totalAmount));
				bill.setId(billId);
				
				try{
					/*生成订单配送费账单*/
					waybillDao.createBillRecord(bill);
					
					/*回写订单have_bill字段为1,表示该订单已经生成账单(防止需要付费订单和账单不对应)*/
					WayBillEntity wb = new WayBillEntity();
					wb.setId(bill.getWaybill_id());
					wb.setCourier_id(bill.getCid());
					wb.setHave_bill("1");
					wb.setWaybill_distance(bill.getWaybill_distance());
					wb.setWaybill_amount(bill.getAmount());
					waybillDao.updateWaybillById(wb);
					
				}catch(Exception e){
					throw new BaseSupportException(e);
				}
			}catch(Exception e){
				throw new BaseSupportException(e);
			}
		}
	}
	
}
