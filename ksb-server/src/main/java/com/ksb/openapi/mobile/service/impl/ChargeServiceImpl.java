package com.ksb.openapi.mobile.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import autonavi.online.framework.sharding.dao.DaoHelper;

import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.ChargeService;
import com.pingplusplus.exception.PingppException;
import com.pingplusplus.model.Charge;

public class ChargeServiceImpl implements ChargeService {

	/*临时计算人民币和送宝B汇率*/
	public double rate = 1.0;
	
	@Override
	public void shipperRecharge(String channelName, String amount,String spId,String pingxxAppId) {
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
		
		String subject = "商家["+spId+"]充值";
		String body = subject+" 人民币 ["+amount+"]分,折合["+kb+"]快宝B";
		
		
        Charge charge = null;
        Map<String, Object> chargeMap = new HashMap<String, Object>();
        chargeMap.put("amount", 100);
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
        app.put("id",pingxxAppId);
        chargeMap.put("app", app);
        try {
            //发起交易请求
            charge = Charge.create(chargeMap);
            System.out.println(charge);
        } catch (PingppException e) {
            throw new BaseSupportException(e.getMessage());
        }
		
		
		
	}

}
