package com.ksb.openapi.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.error.BaseSupportException;

public class AddressUtils {

	/*百度地图 openapi地址和appkey*/
	private static final String BDMAP_URL="http://api.map.baidu.com/geocoder/v2/?";
	private static final String BD_APPKEY="zNC2uIzYGKnY3V8D7iCBbLsi";
	
	/*高德地图 openapi地址和appkey*/
	private final String GDMAP_URL="";
	private final String GD_APPKEY="";
	
	private static Logger log = LogManager.getLogger(AddressUtils.class);
	
	public static ResultEntity batchValidateBuyerAddress(List<BuyerEntity> buyerList){
		
		Map<String, String> returnMap = new HashMap<String, String>();
		ResultEntity rsEntity = new ResultEntity();
		for(BuyerEntity entity : buyerList){
			
			String x = entity.getAddress_x();
			String y = entity.getAddress_y();
			if(StringUtils.isNotBlank(x) && StringUtils.isNotBlank(y)){
				rsEntity.setSuccess(true);
				rsEntity.setObj(x+";"+y);
			}else{
				rsEntity= validateAddressByBDMap(entity.getAddress(),null);
			}
			
			if(!rsEntity.isSuccess()){
				return rsEntity;
			}
			
			/*提取地址对应的经纬度和收货人手机号*/
			returnMap.put(entity.getPhone(), rsEntity.getObj().toString());
		}
		
		rsEntity = new ResultEntity();
		rsEntity.setObj(returnMap);
		rsEntity.setSuccess(true);
		return rsEntity;
	}
	
	
	/**
	 * 基于百度地图 地理编码引擎判断地址是否存在
	 * @param addressInfo
	 * @return
	 */
	public static ResultEntity validateAddressByBDMap(String addressInfo,String city){
		
		ResultEntity rsEntity = new ResultEntity();
		
		if(StringUtils.isBlank(addressInfo)){
			throw new BaseSupportException("未提供验证地址");
		}
		
		if(StringUtils.isBlank(city)){
			city = "";
		}
		
		String addressEncode = null;
		try {
			addressEncode = URLEncoder.encode(addressInfo,"UTF-8");
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			throw new BaseSupportException(e1);
		}
		String url = BDMAP_URL+"ak="+BD_APPKEY+"&output=json&city="+city+"&address="+addressEncode;
		log.debug("baidu_geocoding_url",url);
		String jsonRs = null;
		try{
			jsonRs = HTTPUtils.executeGet(url).getObj().toString();
			log.debug("baidu_geocoding_return",jsonRs);
		}catch(Exception e){}
		try {
			JSONObject jsonObj = JSON.parseObject(jsonRs);

			/* 地理编码请求结果 */
			String status = jsonObj.get("status").toString();
			if (!status.equals("0")) {
				throw new BaseSupportException("地址无法识别");
			}

			/* 状态为0的情况下，判断是否有坐标数据 */
			JSONObject resultObj = jsonObj.getJSONObject("result");
			if (resultObj == null) {
				throw new BaseSupportException("地址无法识别");
			}
			JSONObject coorsObj = resultObj.getJSONObject("location");
			if (coorsObj == null) {
				throw new BaseSupportException("地址无法识别");
			}

			String x = coorsObj.getString("lng");
			String y = coorsObj.getString("lat");
			if (StringUtils.isNotBlank(x) && StringUtils.isNotBlank(y)) {
				rsEntity.setSuccess(true);
				rsEntity.setObj(x+";"+y);
				return rsEntity;
			}
		} catch (Exception e) {
			throw new BaseSupportException("地址无法识别");
		}
		
		rsEntity.setSuccess(false);
		return rsEntity;
	}
	
	/**
	 * 基于高德地图 地理编码引擎判断地址是否存在
	 * @param addressInfo
	 * @return
	 */
	public static boolean validateAddressByGDMap(String addressInfo){
		
		
		return true;
	}
	
	public static void main(String[] args) {
		
		
		//银科大厦 116.31291486161;39.987911010404
		
		//大恒科技大厦 116.31198944614;39.988517521803
		
		ResultEntity r = validateAddressByBDMap("北京市海淀区丰四路68号","北京市");
		System.out.println(r.getObj());
		
	}
	
}
