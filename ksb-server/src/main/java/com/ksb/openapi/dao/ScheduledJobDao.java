package com.ksb.openapi.dao;

import org.springframework.stereotype.Repository;

import autonavi.online.framework.sharding.entry.aspect.annotation.Author;
import autonavi.online.framework.sharding.entry.aspect.annotation.Delete;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select;
import autonavi.online.framework.sharding.entry.aspect.annotation.SingleDataSource;
import autonavi.online.framework.sharding.entry.aspect.annotation.SqlParameter;
import autonavi.online.framework.sharding.entry.aspect.annotation.Update;

@Repository
public class ScheduledJobDao {

	/**
	 * 自动取消指定时间段内，未被分配的订单
	 * @param startTime
	 * @param entTime
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object resetUnAllocateWaybill(@SqlParameter("st")long startTime,@SqlParameter("et")long endTime){
		
		StringBuilder sb = new StringBuilder("update waybill set waybill_status=-3,sys_remarks='等待时间过长,订单被系统定时器自动取消',HANDLE_FLAG=1 ");
		sb.append(" where waybill_status=2 and create_time>=#{st} and create_time<=#{et} and HANDLE_FLAG=0 ");
		
		return sb.toString();
	}
	
	/**
	 * 定时自动处理 配送员没有完成的订单(订单状态为-3)
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update	
	public Object resetUnCompleteWaybill(@SqlParameter("st")long startTime,@SqlParameter("et")long endTime){
		StringBuilder sb = new StringBuilder("update waybill set waybill_status=-3,sys_remarks='定时器自动把配送员未完成的订单调整为取消',HANDLE_FLAG=1 ");
		sb.append(" where (waybill_status>=0 and waybill_status!=5) and create_time>=#{st} and create_time<=#{et} and HANDLE_FLAG=0 ");
		
		return sb.toString();
	}

	/**
	 * 强制重置配送员的 配送状态为空闲
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update	
	public Object resetCourierStatus2free(){
		StringBuilder sb = new StringBuilder("update courier set delivery_status=0 ");
		sb.append(" where delivery_status=1 ");
		return sb.toString();
	}
	
	/**
	 * 扫描指定时间段内未分配的订单
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select		
	public Object scanUnAllocateWaybill(@SqlParameter("st")long startTime,@SqlParameter("et")long endTime){
		StringBuilder sb = new StringBuilder("select id,city_code,waybill_id wb_id,delivery_eids eid,sp_x x,sp_y y from unallocate_waybill where handle_flag=0 and create_time>=#{st} and create_time<=#{et} ");
		return sb.toString();
	}
	
	/**
	 * 同步waybill和unallocate_waybill表数据（把已经有配送员的订单 在unallocate_waybill 中把handle_flag改为1）
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update	
	public Object synWaybillAndUnallocateWaybill(@SqlParameter("st")long startTime,@SqlParameter("et")long endTime){
		
		StringBuilder sb = new StringBuilder("update unallocate_waybill set handle_flag=1 where waybill_id in(");
		sb.append(" select a.id from ( ");
		sb.append(" select wb.id from unallocate_waybill uwb join waybill wb on wb.id = uwb.waybill_id and wb.courier_id is not null and wb.create_time>=#{st} and wb.create_time<=#{et} ");
		sb.append(")a");
		sb.append(")");
		return sb.toString();
	}

	/**
	 * 清除unallocate_waybill表中昨天的数据(防止一直膨大)
	 * @param startTime
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Delete
	public Object delYestodayUnallocateWaybii(@SqlParameter("st") long startTime){
		
		StringBuilder sb = new StringBuilder("delete unallocate_waybill where create_time<#{st}");
		
		return sb.toString();
	}
	
	
}
