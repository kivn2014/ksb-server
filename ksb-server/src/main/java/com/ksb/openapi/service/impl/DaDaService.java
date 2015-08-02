package com.ksb.openapi.service.impl;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.util.HTTPUtils;
import com.ksb.openapi.util.MD5Util;
import com.ksb.openapi.util.SystemConst;

@Service
public class DaDaService {

	private Logger log = LogManager.getLogger(getClass());
	
	private static String DADA_RESTAPI_HOST = "http://public.imdada.cn/";
	private static String DADA_CALLBACK_URL = "";
	
	/*达达rest api nonce*/
	public static final String SIGNATURE_NONCE = "dada";
	
	static{
		try{
			DADA_RESTAPI_HOST = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.DADA_RESTAPI_HOST);
			DADA_CALLBACK_URL = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.DADA_CALLBACK_URL);
		}catch(Exception e){}
	}
	
	/***
	 * 根据达达APP key获取达达restapi授权码
	 * 
	 */
	private String getGrantCode(String appKey){
		
		StringBuffer uri = new StringBuffer(DADA_RESTAPI_HOST+"/oauth/authorize/?");
		
		uri.append("app_key="+appKey);
		uri.append("&scope=dada_base");
		
		String resultStr = null;
		try {
			resultStr = HTTPUtils.executeGet(uri.toString()).getObj().toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("appkey["+appKey+"]获取授权码失败",e);
		}
		
		JSONObject jsonObj = JSON.parseObject(resultStr);
		String resultStatus = jsonObj.get("status").toString();
		
		if(resultStatus.equals("fail")){
			String errorCode = jsonObj.getString("errorCode").toString();
			log.error("获取达达授权码失败,错误码为："+errorCode);
			return null;
		}
		String grantCode = jsonObj.getJSONObject("result").get("grant_code").toString();
		
		return grantCode;
	} 
	
	/**
	 * 获取请求达达openapi的token值
	 */
	public String getDADAToken(String appKey){
		
		/*达达的接口类型参数，表示根据appkey获取rest api请求token*/
		String grantType = "authorization_code";
		
		StringBuffer uri = new StringBuffer(DADA_RESTAPI_HOST+"/oauth/access_token/?");
		uri.append("grant_type="+grantType);
		uri.append("&app_key="+appKey);
		uri.append("&grant_code="+getGrantCode(appKey));
		
		String resultRs = null;
		try {
			resultRs = HTTPUtils.executeGet(uri.toString()).getObj().toString();
		} catch (Exception e1) {
			log.error("appkey["+appKey+"]获取达达token失败",e1);
		}
		if(resultRs==null){
			log.error("无法从达达open api中获取token信息");
			return null;
		}
		
		JSONObject jsonObj = null;
		try{
			jsonObj = JSON.parseObject(resultRs);
		}catch(Exception e){
			log.error("达达open api获取token接口 返回的数据格式异常 "+e.getMessage());
			return null;
		}
		
		String accessToken = jsonObj.getJSONObject("result").get("access_token").toString();
		
		return accessToken;
	}
	
	/**
	 * rest消息体签名
	 * @param token
	 * @param timestamp
	 * @return
	 */
	public String getSignatureByToken(String token,String timestamp){
		
        String[] arrTmp = { token, timestamp, SIGNATURE_NONCE };
        Arrays.sort(arrTmp);  
        StringBuffer sb = new StringBuffer();  
        
        /*将三个参数字符串拼接成一个字符串进行md5加密*/
        for (int i = 0; i < arrTmp.length; i++) {  
            sb.append(arrTmp[i]);  
        }  
        
        /*进行MD5签名*/
        String str = MD5Util.MD5(sb.toString());
        
        return str;
	}
	
	/**
	 * 运单转给达达处理
	 * @param paramMap
	 * @return
	 */
	public String requestCreateDADAOrder(Map<String, String> paramMap){
		
		
		StringBuffer requestUrl = new StringBuffer(DADA_RESTAPI_HOST+"v1_0/addOrder/");
		
		String dadaApiRs = null;
		try {
			dadaApiRs = HTTPUtils.executePost(requestUrl.toString(), paramMap).getObj().toString();
		} catch (Exception e1) {
			log.error("请求达达rest api 提交订单接口失败",e1);
			throw new BaseSupportException(e1);
		}
		
		JSONObject jsonObj = null;
		
		try{
			jsonObj = JSON.parseObject(dadaApiRs);
		}catch(Exception e){
			log.error("达达rest api请求异常: "+e.getMessage());
			throw new BaseSupportException(e);
		}
		
		if(jsonObj.get("status")==null){
			log.error("请求达达rest api创建订单接口返回的结果格式异常");
			throw new BaseSupportException("请求达达rest api创建订单接口返回的结果格式异常");
		}
		
		String status = jsonObj.get("status").toString();
		if(status.equals("fail")){
			log.error("达达rest api请求失败,错误码["+jsonObj.get("errorCode")+"]");
			throw new BaseSupportException("达达rest api请求失败,错误码["+jsonObj.get("errorCode")+"]");
		}
		
		return jsonObj.get("orderid").toString();
	}
	
	/**
	 * 批量把订单提交到达达
	 * @param waybillEntity
	 * @param shipperEntity
	 * @param buyerList
	 */
	public Map<String, String> batchSubmitOrder(WayBillEntity waybillEntity,ShipperEntity shipperEntity,List<BuyerEntity> buyerList){
		
		
		/*根据appp key获取达达token信息(现在的处理方式是 每次都获取一次新的token,之后需要优化为 把tokey和refresh token放入到redis)*/
		String token = getDADAToken(shipperEntity.getDada_appkey());
		
		long timestamps = new Date().getTime();
		
		String sn = getSignatureByToken(token, String.valueOf(timestamps));
		
		/*快送宝运单ID和达达订单ID对应关系*/
		Map<String, String> mapLink = new HashMap<String, String>();
		for(BuyerEntity be : buyerList){
			Map<String, String> pm = convetBean2Map(waybillEntity, shipperEntity, be);
			pm.put("token", token);
			pm.put("timestamp", String.valueOf(timestamps));
			pm.put("signature", sn);
			String dadaOrderId = requestCreateDADAOrder(pm);
			
			/*存储快送宝运单ID和达达订单ID*/
			mapLink.put(be.getWbid(), dadaOrderId);
		}
		return mapLink;
	}
	
	/*把快送宝的javabena对象，转化为达达restapi可以识别的参数格式*/
	private Map<String, String> convetBean2Map(WayBillEntity wb,ShipperEntity se,BuyerEntity be){
		
		Map<String, String> pm = new HashMap<String, String>();
		
		pm.put("origin_id", be.getWbid());
		pm.put("city_name", "");
		pm.put("city_code", "010");
		
		pm.put("pay_for_supplier_fee", wb.getPay_shipper_fee());
		pm.put("fetch_from_receiver_fee", wb.getFetch_buyer_fee());
		pm.put("deliver_fee", "0");
		
		pm.put("create_time", String.valueOf(wb.getCreate_time()));
		pm.put("info", wb.getRemarks());
		pm.put("cargo_type", wb.getCargo_type());
		
		pm.put("cargo_weight", wb.getCargo_weight());
		pm.put("cargo_price", wb.getCargo_price());
		pm.put("cargo_num", wb.getCargo_num());
		
		pm.put("is_prepay", wb.getIs_prepay());
		pm.put("expected_fetch_time", String.valueOf(wb.getExpected_fetch_time()));
		pm.put("expected_finish_time", String.valueOf(wb.getExpected_arrival_time()));

		pm.put("supplier_id", "0");
		pm.put("supplier_name", se.getName());
		pm.put("supplier_address", se.getAddress());
		
		pm.put("supplier_phone", se.getPhone());
		pm.put("supplier_tel", "0");
		pm.put("supplier_lat", "0");	
		pm.put("supplier_lng", "0");
		pm.put("invoice_title", "");
		pm.put("receiver_name", be.getName());	
		
		pm.put("receiver_address", be.getAddress());
		pm.put("receiver_phone", be.getPhone());
		pm.put("receiver_tel", "");	
		
		pm.put("receiver_lat", "0");
		pm.put("receiver_lng", "0");
		pm.put("callback", DADA_CALLBACK_URL);			
		
		return pm;
	}
	
	
	
	
}
