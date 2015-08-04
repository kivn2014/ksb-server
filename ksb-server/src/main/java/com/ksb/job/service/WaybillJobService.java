package com.ksb.job.service;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ksb.openapi.dao.ScheduledJobDao;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.mobile.service.CourierService;
import com.ksb.openapi.util.DateUtil;

@Component
public class WaybillJobService {

	private Logger log = LogManager.getLogger(getClass());
	
	@Autowired
	ScheduledJobDao scheduledJobDao;
	
	@Autowired
	CourierService courierService;
	
	@Autowired
	WaybillDao waybillDao;
	
	@Autowired
	UserDao userDao;
	
	/**
	 * 每10分钟扫描一次是否有空闲的配送员
	 */
	@Scheduled(cron="0 0/10 * * * ? ")
	public void handleUnAllocateWaybill(){
		
		/*从数据库中获取所有未分配的订单*/
		long startTime = DateUtil.getTodayStartTime();
		long endTime = new Date().getTime();
		log.entry(startTime,endTime);
		
		/*首先把unallocate_waybill表和 waybill表做一下同步，防止通过订单分配系统或者其他途径，直接操作waybill表已经把一些未分配的订单分配给配送员*/
		try{
			scheduledJobDao.synWaybillAndUnallocateWaybill(startTime, endTime);
		}catch(Exception e){
			log.error("同步waybill和unallocate_waybill出现异常",e);
		}
		
		log.debug("waybill和unallocate_waybill同步完毕");
		List<Map<String, String>> unAllocateList = null;
		
		try{
			unAllocateList = (List<Map<String, String>>)scheduledJobDao.scanUnAllocateWaybill(startTime, endTime);
		}catch(Exception e){
			log.error("定时加载未分配订单时出现异常",e);
		}
		
		/*数据库中没有未分配的订单*/
		if(unAllocateList==null || unAllocateList.size()==0){
			log.debug("没有未分配的订单");
			return;
		}
		
		//log.debug("未分配的订单数量: "+unAllocateList.size());
		log.debug("未分配的订单---->>"+unAllocateList);
		
		ExecutorService pool = Executors.newFixedThreadPool(10);
		
		/*主线程还在正常的自动分配订单，自动化扫描分配的时候，一定要判断该配送员是否已经有订单了*/
		for(Map<String, String> map : unAllocateList){

			/*使用线程池 批量执行订单的自动分配(线程池最大10 也就是最多同时执行10个线程)*/
			pool.execute(new AllocateWaybill(map));
		}
	}
	
	/**
	 * 多线程处理未分配的订单
	 * @author houshipeng
	 *
	 */
	class AllocateWaybill extends Thread{
		Map<String, String> map;
		public AllocateWaybill(Map<String, String> map){
			this.map = map;
		}
		
		public void run(){
			log.entry("定时器自动处理的订单",map);
			
			//String id = map.get("id");
			
			/*订单ID*/
			String waybillId = map.get("wb_id");

			String eid = map.get("eid");
			String x = map.get("x");
			String y = map.get("y");
		
			/*找空闲的配送员*/
			List<String> courierList = courierService.queryNearCourier(eid, x, y);
			log.debug("查询到附近配送员信息: "+courierList);
			if(courierList==null || courierList.size()==0){
				/*还是没有空闲的配送员*/
				return;
			}
			
			String courierAndEid = courierList.get(0);
			String[] cs = courierAndEid.split("\\^");
			String courierId = cs[0];
			
			/*把订单 分配给配送员(查询条件 waybill表的 courier_id必须为空)*/
			try{
				log.debug("订单["+waybillId+"],分配给["+courierId+"]");
				waybillDao.allocationWayBill2Courier4KSB(waybillId, courierId);
				
				/*配送员状态调整为配送中*/
				userDao.updateCourierDeliveryStatus(courierId, "1");
				
			}catch(Exception e){
				log.error("把订单["+waybillId+"] 分配给["+courierId+"]时出现异常",e);
			}
		}
	}
	
	/*每天 00:05 清理未分配表中昨天的数据(防止表中数据膨胀)*/
	@Scheduled(cron="0 5 0 * * ? ")
	public void clearUnAllocateTableData(){
		
		long todayStartTime = DateUtil.getTodayStartTime();
		log.debug("自动清除unallocate_waybill [create_time < "+todayStartTime+"]之前的数据");
		try{
			scheduledJobDao.delYestodayUnallocateWaybii(todayStartTime);
		}catch(Exception e){
			log.error("自动清除unallocate_waybill表中昨天数据 出现异常",e);
		}
	}
	
	/*每天凌晨 3:30(考虑凌晨可能有配送夜宵的)，处理一些前一天未处理的订单*/
	@Scheduled(cron="0 30 3 * * ? ")
	public void handleYestodayWaybill(){
		
		/*获取昨天时间*/
		long startTime = DateUtil.getYestodayStartTime();
		long endTime = DateUtil.getYesTodayEndTime();
		
		log.entry(startTime,endTime);
		/*处理商家 待接单的数据（全部置为取消）*/
		resetUnAllocateWaybill(startTime,endTime);
		
		/*处理配送员，未配送完成的订单*/
		resetUnComplete(startTime,endTime);
		
		/*所有非空闲的配送员状态统一调整为 空闲*/
		courierWaybillStatus2free();
	}
	
	
	/**
	 * 未分配给配送员的订单
	 */
	void resetUnAllocateWaybill(long startTime,long endTime){
		
		scheduledJobDao.resetUnAllocateWaybill(startTime, endTime);
	}
	
	/**
	 * 配送员未完成的订单
	 */
	void resetUnComplete(long startTime,long endTime){
		
		scheduledJobDao.resetUnCompleteWaybill(startTime, endTime);
	} 
	
	/**
	 * 强制重置配送员 配送状态为空闲
	 */
	void courierWaybillStatus2free(){
		scheduledJobDao.resetCourierStatus2free();
	}
	
	
}
