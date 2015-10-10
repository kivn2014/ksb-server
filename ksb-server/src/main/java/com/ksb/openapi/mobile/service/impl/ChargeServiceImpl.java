package com.ksb.openapi.mobile.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;
import autonavi.online.framework.sharding.dao.DaoHelper;
import autonavi.online.framework.zookeeper.SysProps;

import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.ChargeService;
import com.ksb.openapi.util.SystemConst;
import com.pingplusplus.Pingpp;
import com.pingplusplus.exception.PingppException;
import com.pingplusplus.model.Charge;

@Service
public class ChargeServiceImpl implements ChargeService {

	/*临时计算人民币和送宝B汇率*/
	public double rate = 1.0;
	public static String PINGXX_APP_ID = null;
	
	public ChargeServiceImpl(){
		
		String pingxx_api_key = null;
		try{
			pingxx_api_key = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.PINGXX_API_KEY);
			PINGXX_APP_ID = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.PINGXX_APP_ID);
		}catch(Exception e){}
		
		/*赋值pingxx api key*/
		Pingpp.apiKey = pingxx_api_key;
	}
	@Override
	public Charge shipperRecharge(String channelName, String amount,String spId) {
		// TODO Auto-generated method stub
		
		/*支付渠道*/
		if(StringUtils.isBlank(channelName)){
			throw new BaseSupportException("未指定支付渠道");
		}
		
		/*充值金额*/
		if(StringUtils.isBlank(amount)){
			throw new BaseSupportException("未指定充值金额");
		}
		
		/*充值的商家编号*/
		if(StringUtils.isBlank(spId)){
			throw new BaseSupportException("未指定充值商家");
		}
		
		/*根据最新汇率,人民币转换为快宝B*/
		double kb = Double.parseDouble(amount)*rate;
		
		/*订单金额单位为 分*/
		double rmbFen = Double.parseDouble(amount)*100;
		String subject = "商家["+spId+"]充值";
		String body = subject+" 人民币 ["+rmbFen+"]分,折合["+kb+"]个快宝B";
		
		
        Charge charge = null;
        Map<String, Object> chargeMap = new HashMap<String, Object>();
        chargeMap.put("amount", rmbFen);
        chargeMap.put("currency", "cny");
        chargeMap.put("subject", subject);
        chargeMap.put("body", body);
        try{
        	long orderId = DaoHelper.createPrimaryKey();
        	chargeMap.put("order_no", orderId);
        }catch(Exception e){
        	throw new BaseSupportException("无法生成订单编号");
        }
        chargeMap.put("channel", channelName);
        chargeMap.put("client_ip", "127.0.0.1");
        Map<String, String> app = new HashMap<String, String>();
        app.put("id",PINGXX_APP_ID);
        chargeMap.put("app", app);
        try {
            //发起交易请求
            charge = Charge.create(chargeMap);
        } catch (PingppException e) {
            throw new BaseSupportException(e.getMessage());
        }
		
		return charge;
	}

}
