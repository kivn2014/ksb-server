package com.ksb.openapi.dao;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import autonavi.online.framework.sharding.entry.aspect.annotation.Author;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select;
import autonavi.online.framework.sharding.entry.aspect.annotation.SingleDataSource;
import autonavi.online.framework.sharding.entry.aspect.annotation.SqlParameter;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select.Paging;

@Repository
public class StatisticsDao {

	/**
	 * 按日期和订单状态 分组查询订单基本统计信息
	 * @param enterpriseId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	//@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	@Select
	public Object groupQueryDateStatus(@SqlParameter("ent_id") String enterpriseId,@SqlParameter("st")Long startTime,@SqlParameter("et") Long endTime,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		StringBuilder sb = new StringBuilder("select FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') waybill_date,waybill_status,count(waybill_status) status_num from waybill where THIRD_PLATFORM_ID=#{ent_id}  ");
		
		if(startTime!=null && endTime!=null){
			sb.append(" and (create_time>=#{st} and create_time<=#{et}) ");
		}
		sb.append(" group by waybill_status,FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') order by FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') ");
		return sb.toString();
	}
	
	/**
	 * 按日期和订单状态 分组查询订单总量
	 * @param enterpriseId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select
	public Object groupQueryCountDateStatus(@SqlParameter("ent_id") String enterpriseId,@SqlParameter("st")Long startTime,@SqlParameter("et") Long endTime){
		
		StringBuilder sb = new StringBuilder("select waybill_status,count(waybill_status) status_num from waybill where THIRD_PLATFORM_ID=#{ent_id}  ");
		
		if(startTime!=null && endTime!=null){
			sb.append(" and (create_time>=#{st} and create_time<=#{et}) ");
		}
		sb.append(" group by waybill_status");
		return sb.toString();
	}	
	
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
//	@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	@Select
	public Object queryCourierStatusByDate(@SqlParameter("ent_id") String enterpriseId,@SqlParameter("c_real_name")String courierRealName,@SqlParameter("day_str")String dayStr,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		StringBuilder sb = new StringBuilder("select wb.waybill_status,count(wb.waybill_status) status_num,c.REAL_NAME courier_real_name ");
		sb.append(" from courier c join waybill wb on wb.courier_id=c.id where wb.THIRD_PLATFORM_ID=#{ent_id} ");
		
		if(StringUtils.isNotBlank(courierRealName)){
			sb.append(" and c.real_name like #{c_real_name} ");
		}
		
		if(StringUtils.isNotBlank(dayStr)){
			sb.append(" and FROM_UNIXTIME((create_time/1000),'%Y-%m-%d')=#{day_str} ");
		}
		
		sb.append(" group by wb.waybill_status,c.real_name");
		return sb.toString();
	} 
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select	
	public Object queryShipperStatusByDate(@SqlParameter("sp_id")String spId,@SqlParameter("sp_uid")String spUid,@SqlParameter("st")Long startTime,@SqlParameter("et") Long endTime){
		
		StringBuilder sb = new StringBuilder("select FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') waybill_date,waybill_status,count(waybill_status) status_num,sum(waybill_distance) waybill_distance,sum(waybill_amount) waybill_amount from waybill where shippers_id=#{sp_id} and (waybill_status=5 or waybill_status=-3  ) ");
		
		if(startTime!=null && endTime!=null){
			sb.append(" and (create_time>=#{st} and create_time<=#{et}) ");
		}
		sb.append(" group by waybill_status,FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') order by FROM_UNIXTIME((create_time/1000),'%Y-%m-%d') ");
		return sb.toString();
	}
	
}
