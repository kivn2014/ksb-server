package com.ksb.openapi.dao;

import org.springframework.stereotype.Repository;

import autonavi.online.framework.sharding.dao.constant.ReservedWord;
import autonavi.online.framework.sharding.entry.aspect.annotation.Author;
import autonavi.online.framework.sharding.entry.aspect.annotation.Insert;
import autonavi.online.framework.sharding.entry.aspect.annotation.SingleDataSource;
import autonavi.online.framework.sharding.entry.aspect.annotation.SqlParameter;

import com.ksb.openapi.pay.entity.ChargeEntity;

/**
 * 
 * @author houshipeng
 *
 */
@Repository
public class PayDao {

	/**
	 * 
	 * @param charge
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object saveChargeRecord(@SqlParameter("ce") ChargeEntity charge){
		
		StringBuilder sb = new StringBuilder("insert into charge_record(");
		
		/*字段*/
		sb.append("id,charge_id,charge_object,charge_created,charge_livemode,charge_paid,charge_appid,charge_channel,");
		sb.append("charge_orderno,charge_amount,charge_rcurrency,charge_subject,charge_body,charge_time_paid,channel_transaction_no,");
		sb.append("charge_failure_code,charge_failure_msg,charge_description");
		
		sb.append(") values(");
		
		/*values*/
		sb.append("#{"+ReservedWord.snowflake+"},#{ce.id},#{ce.object},#{ce.created},#{ce.livemode},#{ce.paid},#{ce.appid}");
		
		
		sb.append(")");
		
		return "";
	}
	
	
}
