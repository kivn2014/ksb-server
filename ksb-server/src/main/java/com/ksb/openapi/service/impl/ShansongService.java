package com.ksb.openapi.service.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;

import com.alibaba.fastjson.JSON;
import com.esotericsoftware.minlog.Log;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.HTMLUtils;
import com.ksb.openapi.util.HTTPUtils;
import com.ksb.openapi.util.SystemConst;

@Service
public class ShansongService {

	
	private static String shansongHost = "http://www.bingex.com/";
	
	/**
	 * 组装数据，把运单提交到闪送
	 * @param x
	 * @param y
	 * @param waybillEntity
	 * @param buyerList
	 * @param groupList
	 */
	public Map<String, List<String>> batchSubmitShansongOrder(ShipperEntity shipperEntity,WayBillEntity waybillEntity,List<BuyerEntity> buyerList,List<List<String>> groupList){
		
		/*订单提交前，数据转换工作*/
		Map<String, BuyerEntity> buyerMap = new HashMap<String, BuyerEntity>();
		for(BuyerEntity be : buyerList){
			buyerMap.put(be.getWbid(), be);
		}
		
		/*需要记录，闪送订单id和快送宝运单id 对应关系*/
		Map<String, List<String>> rsMap = new HashMap<String, List<String>>();
		
		/*根据动态分单结果，开始处理数据*/
		for(List<String> list : groupList){
			String shansongOrderId = handleShansong(shipperEntity,waybillEntity, converGroupOrder(buyerMap, list));
			rsMap.put(shansongOrderId, list);
		}
		
		return rsMap;
	}
	
	
	private List<BuyerEntity> converGroupOrder(Map<String, BuyerEntity> buyerMap,List<String> groupList){
		
		List<BuyerEntity> returnList = new ArrayList<BuyerEntity>();
		for(String id : groupList){
			returnList.add(buyerMap.get(id));
		}
		return returnList;
	}
	
	/**
	 * 向闪送提交订单
	 * @param shipperEntity
	 * @param waybillEntity
	 * @param buyerList
	 */
	private String handleShansong(ShipperEntity shipperEntity,WayBillEntity waybillEntity,List<BuyerEntity> buyerList){
		
		/*数据转换为闪送可识别的 数据格式*/
		
		/*拼装一单多个地址中，多个地址数据格式*/
		/*计算费用的接口*/
		StringBuilder feeAddress = new StringBuilder("[");
		/*预提交订单接口*/
		StringBuilder preCommitAddress = new StringBuilder("[");
		
		for(BuyerEntity be : buyerList){
			String name = be.getName();
			String phone = be.getPhone();
			String bx = be.getAddress_x();
			String by = be.getAddress_y();
			
			feeAddress.append("{longitude: "+bx+", latitude: "+by+"},");
			preCommitAddress.append("{name: \""+urlEncode(name)+"\", mobile: \""+urlEncode(phone)+"\", city: \""+urlEncode(waybillEntity.getCity_name())+"\", longitude: "+shipperEntity.getAddress_x()+", latitude: "+shipperEntity.getAddress_y()+", address: \""+urlEncode(shipperEntity.getAddress()) +", demo: \""+urlEncode(waybillEntity.getRemarks()) +"\"}");
		}
		
		String fee2Address = feeAddress.substring(0, feeAddress.length()-1)+"]";
		String precommit2Address = preCommitAddress.substring(0, preCommitAddress.length()-1)+"]";
		
		/*订单费用计算参数*/
		Map<String, String> feeMap = new HashMap<String, String>();
		feeMap.put("fromAddressObjectJson", "{longitude:"+shipperEntity.getAddress_x()+", latitude:"+shipperEntity.getAddress_y()+",city:"+urlEncode(waybillEntity.getCity_name())+"}");
		feeMap.put("toAddressObjectList", fee2Address);
		feeMap.put("appointType", "1");
		feeMap.put("weight", waybillEntity.getCargo_weight());
		
		/*根据实时计费接口获取订单所有的公里数(结果要 /1000)*/
		
		String feeUri = "web/order/dynamicfee";
		/*计算改订单的公里数和费用*/
		String feeJson = null;
		try {
			feeJson = post(feeUri, feeMap);
		} catch (Exception e) {
			Log.error("实时费用计算异常",e);
			throw new BaseSupportException(e);
		}
		
		String distanceStr = JSON.parseObject(feeJson).getJSONObject("data").getString("distance");
		String shippingFee = JSON.parseObject(feeJson).getJSONObject("data").getString("shippingFee");
		String payAmount = JSON.parseObject(feeJson).getJSONObject("data").getString("payAmount");
		
		/*距离单位为km*/
		String orderDistance = String.valueOf(Double.parseDouble(distanceStr)/1000);
		
		
		/*预提交订单所需要的参数*/
		Map<String, String> precommitMap = new HashMap<String, String>();
		String pickup_info = "{name: \""+urlEncode(shipperEntity.getName())+"\", mobile: \""+urlEncode(shipperEntity.getPhone())+"\", city: \""+urlEncode(waybillEntity.getCity_name())+"\", longitude: "+shipperEntity.getAddress_x()+", latitude: "+shipperEntity.getAddress_y()+", address: \""+urlEncode(shipperEntity.getAddress())+"\"}";
		
		precommitMap.put("fromAddressObjectJson", pickup_info);
		precommitMap.put("fromProvince", waybillEntity.getProvince_name());
		precommitMap.put("fromCity", waybillEntity.getCity_name());
		precommitMap.put("toAddressObjectJson", precommit2Address);
		precommitMap.put("distance", orderDistance);
		precommitMap.put("appointType", waybillEntity.getBooking_fetch());
		precommitMap.put("appointmentDate", DateUtil.longDate2String(waybillEntity.getBooking_fetch_time(),"yyyy-MM-dd"));
		precommitMap.put("appointmentTime", DateUtil.longDate2String(waybillEntity.getBooking_fetch_time(),"HH:mm"));
		precommitMap.put("weight", waybillEntity.getCargo_weight());
		precommitMap.put("goodsName", waybillEntity.getCargo_num());
		precommitMap.put("demo", waybillEntity.getRemarks());
		precommitMap.put("fromChannel", "1");
		
		/*预提交闪送订单，获取订单ID(订单ID是之后确认提交的依据)*/
		String shansongOrderId = null;
		try{
			shansongOrderId = post("web/order/precommit",precommitMap);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		/*闪送订单确认提交*/
		String shansongName = shipperEntity.getShansong_name();
		String shansongPasswd = shipperEntity.getShansong_password();
		confirmShansongOrder(shansongName,shansongPasswd,shansongOrderId,payAmount);
		
		return shansongOrderId;
	}
	
	/**
	 * url encode编码
	 * @param s
	 * @return
	 */
	private String urlEncode(String s){
		String encodeStr = null;
		try {
			encodeStr = URLEncoder.encode(s, "utf-8");
		} catch (UnsupportedEncodingException e) {
			return s;
		}
		return encodeStr;
	}
	
	/**
	 * 闪送订单预提交
	 * @param uri
	 * @param paramMap
	 */
	private String post(String uri,Map<String, String> paramMap){
		
		/*从配置文件中获取闪送的Host地址*/
		try {
			shansongHost = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.SHANSONG_HOST);
		} catch (Exception e) {/*如果从配置文件中获取异常，则使用方法中默认的地址*/}
		String postUrl = shansongHost+uri;
		
		ResultEntity rsEntity = null;
		try {
			rsEntity = HTTPUtils.executePost(postUrl, paramMap);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new BaseSupportException(e);
		}
		
		String httpRsCode = rsEntity.getErrors();
		if(!httpRsCode.equals("200")){
			/*http请求失败*/
			throw new BaseSupportException("预提交接口，http返回错误码["+httpRsCode+"]");
		}
		return rsEntity.getObj().toString();
	}
	
	
	private void confirmShansongOrder(String userName,String password,String shansongOrderId,String payAmount){

		/*从配置文件中获取闪送的Host地址*/
		try {
			shansongHost = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.SHANSONG_HOST);
		} catch (Exception e) {/*如果从配置文件中获取异常，则使用方法中默认的地址*/}
		
		String urlLogin = shansongHost+"user/doLogin?service=&username=" + userName+ "&password=" + password;
		
		try {
			DefaultHttpClient client = new DefaultHttpClient(
					new PoolingClientConnectionManager());

			/**
			 * 第一次请求登录页面 获得cookie 相当于在登录页面点击登录，此处在URL中 构造参数，
			 * 如果参数列表相当多的话可以使用HttpClient的方式构造参数 此处不赘述
			 */
			HttpPost post = new HttpPost(urlLogin);
			HttpResponse response = client.execute(post);
//			HttpEntity entity = response.getEntity();
			CookieStore cookieStore = client.getCookieStore();
			client.setCookieStore(cookieStore);

			Header locationHeader = response.getLastHeader("Location");
			String location = null;
			if (locationHeader != null) {
				location = locationHeader.getValue();

				HttpGet get = new HttpGet(location);
				response = client.execute(get);
//				entity = response.getEntity();
			}

			String confirmUrl = shansongHost + "web/order/confirm?orderNumber="
					+ shansongOrderId;

			HttpGet get = new HttpGet(confirmUrl);

			CookieStore cookieStore3 = client.getCookieStore();
			client.setCookieStore(cookieStore3);
			response = client.execute(get);
			String confirmStr = EntityUtils.toString(response.getEntity());
			CookieStore cookieStore2 = client.getCookieStore();
			client.setCookieStore(cookieStore2);

			String submitUrl = shansongHost + "web/order/submit";
			HttpPost post2 = new HttpPost(submitUrl);

			/*从页面抓取接口的 token信息，以及其他动态信息*/
			Map<String, String> formMap = HTMLUtils.converFormParam2Map(confirmStr);
			formMap.put("useBalance", "true");
			formMap.put("useAmount", payAmount);
			formMap.put("dymatic_pwd", "0");
			
			/*仅支持使用余额 全额支付，不支持使用优惠券*/
			formMap.put("paymentType", "2");
			HttpEntity paramEntity = new UrlEncodedFormEntity(
					converMap2PairList(formMap));
			post2.setEntity(paramEntity);
			post2.setHeader("Referer", confirmUrl);
			post2.setHeader(
					"User-Agent",
					"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:38.0) Gecko/20100101 Firefox/38.0");

			client.execute(post2);
		} catch (Exception e) {}

	}
	
	public List<NameValuePair> converMap2PairList(Map<String, String> pm){
		
		List<NameValuePair> paramList = new ArrayList<NameValuePair>();
		
		if(pm==null||pm.size()==0){
			return paramList;
		}
		
		Iterator<Entry<String, String>> it = pm.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, String> entry = it.next();
			paramList.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}
		return paramList;
	}
}
