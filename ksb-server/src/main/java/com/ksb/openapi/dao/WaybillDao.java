package com.ksb.openapi.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import com.esotericsoftware.minlog.Log;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.util.DateUtil;

import autonavi.online.framework.sharding.dao.DaoHelper;
import autonavi.online.framework.sharding.dao.constant.ReservedWord;
import autonavi.online.framework.sharding.entry.aspect.annotation.Author;
import autonavi.online.framework.sharding.entry.aspect.annotation.Insert;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select;
import autonavi.online.framework.sharding.entry.aspect.annotation.SingleDataSource;
import autonavi.online.framework.sharding.entry.aspect.annotation.SqlParameter;
import autonavi.online.framework.sharding.entry.aspect.annotation.Update;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select.Paging;
import autonavi.online.framework.sharding.entry.entity.CollectionType;

/**
 * 运单管理DAO
 *         运单状态： 0 初始化；1 分配给配送员；2 商家取货; 3 配送中; 5 完成配送
 *                  -1 运单取消；-2 拒收
 * @author houshipeng
 *
 */

@Repository
public class WaybillDao {

	/**
	 * 运单数据入库
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createKSBWayBill(@SqlParameter("m") Map<String, String> map){
		
//		map.put("create_time", String.valueOf(new Date().getTime()));
		map.put("waybill_status", "0");
		map.put("payment_status", "0");
		
		StringBuffer sb=new StringBuffer("insert into waybill(");
		
		sb.append("id,shipper_origin_id,third_party_waybill_id,courier_id,shippers_id,buyer_id,cargo_type,cargo_weight,cargo_price,"
				+ "cargo_num,create_time,expected_fetch_time,expected_arrival_time,province_name,city_name,pay_type,is_prepay,");
		sb.append("waybill_status,payment_status,waybill_fee,pay_shipper_fee,fetch_buyer_fee,discount_fee,third_platform_id,waybill_type");
		
		sb.append(") values(");
		
		sb.append("#{m.id},#{m.shipper_origin_id},#{m.third_party_id},"
				+ "#{m.courier_id},#{m.shippers_id},#{m.buyer_id},");
		
		sb.append("#{m.cargo_type},#{m.cargo_weight},#{m.cargo_price},#{m.cargo_num},"
				+ "(UNIX_TIMESTAMP() * 1000),#{m.expected_fetch_time},#{m.expected_arrival_time},");
		
		sb.append("#{m.province_name},#{m.city_name},#{m.pay_type},#{m.is_prepay},"
				+ "#{m.waybill_status},#{m.payment_status},#{m.waybill_fee},"
				+ "#{m.pay_shipper_fee},#{m.fetch_buyer_fee},#{m.discount_fee},#{m.third_platform_id},2");
		sb.append(")");
		
		return sb.toString();
	}

	/**
	 * 同一个商家，多个收货人的快送宝运单入库(主要用在openapi，商家一次导入多个运单)
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object batchCreateKSBWayBillByBuyerAddress(@SqlParameter("m") WayBillEntity waybill, @SqlParameter("list") List<BuyerEntity> buyerList){
		
		StringBuffer sb=new StringBuffer("insert into waybill(");
		
		sb.append("id,shipper_origin_id,third_party_waybill_id,courier_id,shippers_id,buyer_id,cargo_type,cargo_weight,cargo_price,"
				+ "cargo_num,create_time,expected_fetch_time,expected_arrival_time,province_name,city_name,pay_type,is_prepay,");
		sb.append("waybill_status,payment_status,waybill_fee,pay_shipper_fee,fetch_buyer_fee,discount_fee,third_platform_id,is_topay,remarks,waybill_type");
		
		sb.append(") values(");
		
		sb.append("#{list."+ReservedWord.index+".wbid},#{m.shipper_origin_id},#{m.third_party_waybill_id},"
				+ "#{m.courier_id},#{m.shippers_id},#{list."+ReservedWord.index+".id},");
		
		sb.append("#{m.cargo_type},#{m.cargo_weight},#{m.cargo_price},#{m.cargo_num},"
				+ "(UNIX_TIMESTAMP() * 1000),#{m.expected_fetch_time},#{m.expected_arrival_time},");
		
		sb.append("#{m.province_name},#{m.city_name},#{m.pay_type},#{m.is_prepay},"
				+ "#{m.waybill_status},#{m.payment_status},#{m.waybill_fee},"
				+ "#{m.pay_shipper_fee},#{m.fetch_buyer_fee},#{m.discount_fee},#{m.third_platform_id},#{m.is_topay},#{m.remarks},#{m.waybill_type}");
		sb.append(")");
		
		return sb.toString();
	}
	
	
	/**
	 * 批量快送宝运单数据入库
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object batchCreateKSBWayBill(@SqlParameter("list") List<WayBillEntity> waybillList){
		
		
		StringBuffer sb=new StringBuffer("insert into waybill(");
		
		sb.append("id,shipper_origin_id,third_party_waybill_id,courier_id,shippers_id,buyer_id,cargo_type,cargo_weight,cargo_price,"
				+ "cargo_num,create_time,expected_fetch_time,expected_arrival_time,province_name,city_name,city_code,pay_type,is_prepay,");
		sb.append("waybill_status,payment_status,waybill_fee,pay_shipper_fee,fetch_buyer_fee,discount_fee,third_platform_id,is_topay,remarks,waybill_type,waybill_num");
		
		sb.append(") values(");
		
		sb.append("#{list."+ReservedWord.index+""
				        + ".id},"
				+ "#{list."+ReservedWord.index+""
						+ ".shipper_origin_id},"
				+ "#{list."+ReservedWord.index+""
						+ ".third_party_waybill_id},"
				+ "#{list."+ReservedWord.index+""
						+ ".courier_id},"
				+ "#{list."+ReservedWord.index+""
						+ ".shippers_id},"
				+ "#{list."+ReservedWord.index+""
						+ ".buyer_id},");
		
		sb.append("#{list."+ReservedWord.index+""
				        + ".cargo_type},"
				+ "#{list."+ReservedWord.index+""
						+ ".cargo_weight},"
				+ "#{list."+ReservedWord.index+""
						+ ".cargo_price},"
				+ "#{list."+ReservedWord.index+""
						+ ".cargo_num},"
				+ "(UNIX_TIMESTAMP() * 1000),"
				+ "#{list."+ReservedWord.index+""
						+ ".expected_fetch_time},"
				+ "#{list."+ReservedWord.index+""
						+ ".expected_arrival_time},");
		
		sb.append("#{list."+ReservedWord.index+""
				        + ".province_name},"
				+ "#{list."+ReservedWord.index+""
						+ ".city_name},"
				+ "#{list."+ReservedWord.index+""
						+ ".city_code},"				
				+ "#{list."+ReservedWord.index+""
						+ ".pay_type},"
				+ "#{list."+ReservedWord.index+""
						+ ".is_prepay},"
				+ "#{list."+ReservedWord.index+""
						+ ".waybill_status},"
				+ "#{list."+ReservedWord.index+""
						+ ".payment_status},"
				+ "#{list."+ReservedWord.index+""
						+ ".waybill_fee},"
				+ "#{list."+ReservedWord.index+""
						+ ".pay_shipper_fee},"
				+ "#{list."+ReservedWord.index+""
						+ ".fetch_buyer_fee},"
				+ "#{list."+ReservedWord.index+""
						+ ".discount_fee},"
				+ "#{list."+ReservedWord.index+""
						+ ".third_platform_id},"
				+ "#{list."+ReservedWord.index+""
						+ ".is_topay},"
				+ "#{list."+ReservedWord.index+""
						+ ".remarks},"
				+ "#{list."+ReservedWord.index+""
						+ ".waybill_type},"
				+ "#{list."+ReservedWord.index+""
						+ ".waybill_num}");
		
		sb.append(")");
		return sb.toString();
	}
	
	
	/**
	 * 运单分配给快递员(通过快送宝运单ID)
	 * @param ksbWayBillId
	 * @param courierId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object allocationWayBill2Courier4KSB(@SqlParameter("ksb_waybill_id") String ksbWayBillId,@SqlParameter("courier_id") String courierId){
		
		/*增加判断条件  运单状态为0(表示运单未分配)*/
		StringBuilder sb = new StringBuilder("update waybill set waybill_status=0,courier_id=#{courier_id}" );
		sb.append(" where id=#{ksb_waybill_id} and waybill_status=2 and courier_id is null ");
		
		return sb.toString();
	}
	
	
	/**
	 * 快送宝的运单分配到第三方配送平台
	 * @param wayBillId 快送宝运单ID
	 * @param thirdPlatformId 第三方平台编号
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object allocationWayBill2ThirdParty(@SqlParameter("waybill_id") String wayBillId,@SqlParameter("third_platform_id") String thirdPlatformId,@SqlParameter("third_order_id") String thirdOrderId){
		
		StringBuilder sb = new StringBuilder("update waybill set THIRD_PARTY_WAYBILL_ID=#{third_order_id},THIRD_PLATFORM_ID=#{third_platform_id} where id=#{waybill_id}");
		
		return sb.toString();
	}

	/**
	 * 快送宝的运单分配到第三方配送平台(支持批量把快送宝运单分配给第三方配送平台)
	 * @param wayBillId 快送宝运单ID
	 * @param thirdPlatformId 第三方平台编号
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object batchAllocationWayBill2ThirdParty(@SqlParameter("list") List<String> wayBillId,@SqlParameter("third_platform_id") String thirdPlatformId,@SqlParameter("third_order_id") String thirdOrderId){
		
		StringBuilder sb = new StringBuilder("update waybill set THIRD_PARTY_WAYBILL_ID=#{third_order_id},THIRD_PLATFORM_ID=#{third_platform_id} where id=#{list."+ReservedWord.index+"}");
		return sb.toString();
	}
	
	/**
	 * 商家自助平台查询运单信息
	 * @param shippersId 商家 ID
	 * @param shipperOrderId 商家自己运单ID
	 * @param wayBillStatus 运单当前状态
	 * @param buyerPhone 买家手机号码
	 * @param buyerName  买家姓名
	 * @deprecated
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select
	//@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object searchWaybillInfo(@SqlParameter("shippers_id") String shippersId,
			@SqlParameter("shippers_order_id") String shipperOrderId,
			@SqlParameter("waybill_status") String wayBillStatus,
			@SqlParameter("buyer_phone") String buyerPhone,
			@SqlParameter("buyer_name") String buyerName
			){
		
		// select
		// wb.id 运单编号,sp.name 商家名称,sp.address 商家地址,sp.phone 商家手机,
		// u.name 买家名称,u.address 买家地址,u.phone 买家手机,c.name 配送员,c.phone 配送员手机
		// from courier c join waybill wb on c.id = wb.courier_id join user u on
		// u.id=wb.buyer_id join shippers sp on sp.id=wb.shippers_ID
		//	
		
//		 select wb.id 运单编号,tpd.name 第三方配送平台,sp.name 商家名称,sp.address 商家地址,sp.phone 商家手机,
//		 u.name 买家名称,u.address 买家地址,u.phone 买家手机,c.name 配送员,c.phone 配送员手机,wb.payment_status 支付状态,wb.pay_type 支付方式,
//         wb.IS_PREPAY 是否垫付,wb.CARGO_TYPE 货物类型,wb.CARGO_WEIGHT 货物重量,wb.CARGO_PRICE 货物价格,wb.CARGO_NUM 货物数量
//		 from courier c join waybill wb on c.id = wb.courier_id join user u on
//		 u.id=wb.buyer_id join shippers sp on sp.id=wb.shippers_ID join third_party_delivery tpd on wb.THIRD_PLATFORM_ID = tpd.id
         
		 StringBuilder sb = new StringBuilder("select ");
		 
		 sb.append(" wb.id id,wb.waybill_status status ,tpd.name third_platform_name,wb.shipper_origin_id shipper_origin_id,sp.name shipper_name,sp.address shipper_address,sp.phone shipper_phone, ");
		 sb.append(" u.name buyer_name,u.address buyer_address,u.phone buyer_phone,c.name courier_name,c.phone courier_phone,wb.payment_status payment_status,wb.pay_type pay_type, ");
		 sb.append(" wb.IS_PREPAY is_prepay,wb.CARGO_TYPE cargo_type,wb.CARGO_WEIGHT cargo_weight,wb.CARGO_PRICE cargo_price,wb.CARGO_NUM cargo_num ,wb.create_time create_time,wb.finish_time finish_time");
		 
		 sb.append(" from courier c join waybill wb on c.id = wb.courier_id join user u on ");
		 sb.append(" u.id=wb.buyer_id join shippers sp on sp.id=wb.shippers_ID ");
		 sb.append(" join enterprise tpd on wb.THIRD_PLATFORM_ID = tpd.id ");
		 
		 sb.append(" where 1=1 ");
		 if(StringUtils.isNotBlank(shippersId)){
			 sb.append(" and sp.id=#{shippers_id} ");
		 }
		 
		 if(StringUtils.isNotBlank(shipperOrderId)){
			 sb.append(" and  shipper_origin_id=#{shippers_order_id}");
		 }
		
		 if(StringUtils.isNotBlank(wayBillStatus)){
			 sb.append(" and waybill_status=#{waybill_status} ");
		 }
		
		 if(StringUtils.isNotBlank(buyerName)){
			 sb.append(" and u.name like '%${buyer_name}%' ");
		 }
		 if(StringUtils.isNotBlank(buyerPhone)){
			 sb.append(" and u.phone =#{buyer_phone} ");
		 }
		
		return sb.toString();
	}
	
	/**
	 * 配送员查询运单数据(初期演示demo页面接口)
	 * @deprecated 遗留接口，新接口参考queryCourierTodayOLWaybillList
	 * @param courierId
	 * @param waybillId
	 * @param waybillStatus
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object searchWaybillInfoByCourier(@SqlParameter("cid") String courierId,@SqlParameter("waybill_id") String waybillId,@SqlParameter("waybill_status") String waybillStatus,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		 StringBuilder sb = new StringBuilder();
		 sb.append("select ");
		
		 sb.append(" wb.id id,wb.waybill_status status ,sp.name shipper_name,sp.address shipper_address,sp.phone shipper_phone, ");
		 sb.append(" u.name buyer_name,u.address buyer_address,u.phone buyer_phone,wb.payment_status payment_status,wb.pay_type pay_type, ");
		 sb.append(" wb.IS_PREPAY is_prepay,wb.create_time create_time,wb.finish_time finish_time,wb.EXPECTED_FETCH_TIME fetch_time,wb.EXPECTED_ARRIVAL_TIME arrival_time");
		 
		 sb.append(" from courier c join waybill wb on c.id = wb.courier_id join user u on ");
		 sb.append(" u.id=wb.buyer_id join shippers sp on sp.id=wb.shippers_ID ");
		 
		 sb.append(" where 1=1 and c.id=#{cid} ");
		 
		 if(StringUtils.isNotBlank(waybillStatus)){
			 sb.append(" and wb.waybill_status=#{waybill_status} ");
		 }
		 
		 if(StringUtils.isNotBlank(waybillId)){
			 sb.append(" and wb.id=#{waybill_id} ");
		 }
		 
		 sb.append(" order by wb.create_time desc");
		 
		 return sb.toString();
		
	}

	/**
	 * 配送员查询运单数据(配送员APP 电商运单数据展示)
	 * @param courierId
	 * @param waybillId
	 * @param waybillStatus
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.mapList,paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object queryCourierTodayOLWaybillList(@SqlParameter("cid") String courierId,@SqlParameter("waybill_id") String waybillId,@SqlParameter("status") String waybillStatus,@SqlParameter("t")String waybillType,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		long startTime = DateUtil.getTodayStartTime();
		long endTime = DateUtil.getTodayEndTime();
		
		 StringBuilder sb = new StringBuilder();
		 sb.append("select wb.id,wb.shipper_origin_id,wb.pay_shipper_fee,wb.waybill_status,wb.is_topay,wb.cargo_price,sp.name shipper_name,FROM_UNIXTIME((wb.create_time/1000),'%Y-%m-%d %T') create_time,ifnull(wb.remarks,'') remarks,ifnull(wb.fetch_buyer_fee,'0') fetch_buyer_fee");
		
		 sb.append(" from waybill wb join shippers sp on sp.id=wb.shippers_ID ");
		 sb.append(" where 1=1 and wb.courier_id=#{cid} ");
		 
		 if(StringUtils.isNotBlank(waybillType)){
			 sb.append(" and wb.waybill_type=#{t} ");
		 }
		 
		 if(StringUtils.isNotBlank(waybillStatus)){
			 if(waybillStatus.equals("e")){
				 sb.append(" and (wb.waybill_status=-1 or wb.waybill_status=-2) ");
			 }else{
				 sb.append(" and wb.waybill_status=#{status} ");
			 }
		 }
		 
		 if(StringUtils.isNotBlank(waybillId)){
			 sb.append(" and wb.id=#{waybill_id} ");
		 }
		 
		 sb.append(" and wb.create_time>="+startTime+" and wb.create_time<="+endTime+" ");
		 sb.append(" order by wb.create_time desc");
		 return sb.toString();
	}

	/**
	 * 配送员查询运单数据(配送员APP O2O运单数据展示)
	 * @param courierId
	 * @param waybillId
	 * @param waybillStatus
	 * @param waybillType
	 * @param skip
	 * @param size
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.mapList,paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object queryCourierTodayO2OWaybillList(@SqlParameter("cid") String courierId,@SqlParameter("waybill_id") String waybillId,@SqlParameter("status") String waybillStatus,@SqlParameter("t")String waybillType,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		/**
		 * 关注的字段：
		 * 
		 * id
		 * 状态
		 * 支付状态
		 * 支付类型
		 * 发货人姓名/发货人电话
		 * 发货人地址
		 * 发货人地址明细
		 * 发货人经纬度
		 * 
		 * 收货人姓名/收货人电话(可以为空)
		 * 收货人地址(可以为空)
		 * 收货人经纬度(可以为空)
		 * 
		 * 是否预定
		 * 预定时间
		 * 下单时间
		 * 
		 * 货物类型
		 * 货物数量
		 * 总单量
		 * 
		 * 是否需要垫付
		 * 向商家付款数
		 * 向客户收款数
		 * 商品价格
		 * 备注
		 * 
		 * 
		 */
		 long startTime = DateUtil.getTodayStartTime();
		// long startTime = 0;
		 long endTime = DateUtil.getTodayEndTime();
		
		 StringBuilder sb = new StringBuilder();
		 sb.append("select wb.id,wb.waybill_status,sp.name shipper_name,sp.phone shipper_phone,sp.address shipper_address,ifnull(sp.address_detail,'') shipper_address_detail,sp.address_x shipper_x,sp.address_y shipper_y,");
		 sb.append(" ifnull(u.name,'未填') buyer_name,ifnull(u.real_name,'') buyer_real_name,ifnull(u.phone,'') buyer_phone,ifnull(u.address,'') buyer_address,ifnull(u.address_x,'0') buyer_x,ifnull(u.address_y,'0') buyer_y,");
		 sb.append(" wb.booking_fetch is_booking,ifnull(FROM_UNIXTIME((wb.booking_fetch_time/1000),'%Y-%m-%d %H:%i'),'') booking_fetch_time,FROM_UNIXTIME((wb.create_time/1000),'%Y-%m-%d %T') create_time,FROM_UNIXTIME((wb.finish_time/1000),'%Y-%m-%d %T') finish_time, ");
		 sb.append(" wb.is_prepay is_prpay,ifnull(wb.pay_shipper_fee,'0') pay_shipper_fee,ifnull(wb.fetch_buyer_fee,'0') fetch_buyer_fee,wb.cargo_price,wb.waybill_fee waybill_fee,ifnull(wb.remarks,'') remarks,wb.pay_type pay_type,wb.payment_status payment_status, ");
		 sb.append(" wb.cargo_type,wb.cargo_num,wb.waybill_num ");
		 
		 /*上缴费用*/
		 //sb.append(" CASE WHEN (pay_type=2) THEN 0 ELSE ifnull((convert(FETCH_BUYER_FEE , SIGNED INTEGER)-convert(PAY_SHIPPER_FEE ,SIGNED INTEGER)),0) END handover_fee ");
		 
		 //sb.append(" from user u join waybill wb on u.id = wb.buyer_id join shippers sp on sp.id=wb.shippers_ID ");
		 sb.append(" from shippers sp join waybill wb on sp.id=wb.shippers_ID left join user u on u.id = wb.buyer_id ");
		
		 sb.append(" where 1=1 and wb.courier_id=#{cid} ");
		 
		 if(StringUtils.isNotBlank(waybillType)){
			 sb.append(" and wb.waybill_type=#{t} ");
		 }
		 
		 if(StringUtils.isNotBlank(waybillStatus)){
			 if(waybillStatus.equals("e")){
				 sb.append(" and (wb.waybill_status=-1 or wb.waybill_status=-2) ");
			 }else{
				 sb.append(" and wb.waybill_status=#{status} ");
			 }
		 }
		 
		 if(StringUtils.isNotBlank(waybillId)){
			 sb.append(" and wb.id=#{waybill_id} ");
		 }
		 
		 sb.append(" and wb.create_time>="+startTime+" and wb.create_time<="+endTime+" ");
		 sb.append(" order by wb.create_time desc");
		 return sb.toString();
	}

	/**
	 * 商家查询运单数据(商家版APP O2O运单数据展示)
	 * @param courierId
	 * @param waybillId
	 * @param waybillStatus
	 * @param waybillType
	 * @param skip
	 * @param size
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object queryShipperO2OWaybillList(@SqlParameter("sp_id") String shipperId,@SqlParameter("waybill_id") String waybillId,@SqlParameter("status") String waybillStatus,@SqlParameter("t")String waybillType,@SqlParameter("st") Long startTime,@SqlParameter("st") Long endTime,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		/**
		 * 关注的字段：
		 * 
		 * id
		 * 状态
		 * 支付状态
		 * 支付类型
		 * 发货人姓名/发货人电话
		 * 发货人地址
		 * 发货人经纬度
		 * 
		 * 收货人姓名/收货人电话
		 * 收货人地址
		 * 收货人经纬度
		 * 
		 * 货物类型
		 * 货物数量
		 * 总单量
		 * 
		 * 是否预定
		 * 预定时间
		 * 下单时间
		 * 
		 * 是否需要垫付
		 * 向商家付款数
		 * 向客户收款数
		 * 商品价格
		 * 备注
		 * 
		 * 配送员名称
		 * 配送员手机号
		 * 
		 */
		 StringBuilder sb = new StringBuilder();
		 sb.append("select wb.id,wb.waybill_status,sp.name shipper_name,sp.phone shipper_phone,sp.address shipper_address,sp.address_x shipper_x,sp.address_y shipper_y,c.real_name courier_real_name,c.phone courier_phone,");
		 sb.append(" u.name buyer_name,u.real_name buyer_real_name,u.phone buyer_phone,u.address buyer_address,u.address_x buyer_x,u.address_y buyer_y,");
		 sb.append(" wb.booking_fetch is_booking,ifnull(FROM_UNIXTIME((wb.booking_fetch_time/1000),'%Y-%m-%d %H:%i'),'') booking_fetch_time,FROM_UNIXTIME((wb.create_time/1000),'%Y-%m-%d %T') create_time,FROM_UNIXTIME((wb.finish_time/1000),'%Y-%m-%d %T') finish_time, ");
		 sb.append(" wb.is_prepay is_prpay,ifnull(wb.pay_shipper_fee,'0') pay_shipper_fee,ifnull(wb.fetch_buyer_fee,'0') fetch_buyer_fee,wb.cargo_price,wb.cargo_type,wb.cargo_num,wb.waybill_num,wb.waybill_fee waybill_fee,ifnull(wb.remarks,'') remarks,wb.pay_type pay_type,wb.payment_status payment_status ");
		 //sb.append(" CASE WHEN (pay_type=2) THEN 0 ELSE ifnull((convert(FETCH_BUYER_FEE , SIGNED INTEGER)-convert(PAY_SHIPPER_FEE ,SIGNED INTEGER)),0) END handover_fee ");
		 
		 sb.append(" from shippers sp join waybill wb on sp.id=wb.shippers_ID left join courier c on c.id = wb.courier_id left join user u on u.id = wb.buyer_id ");
		
		 sb.append(" where 1=1 and wb.shippers_id=#{sp_id} ");
		 
		 if(StringUtils.isNotBlank(waybillType)){
			 sb.append(" and wb.waybill_type=#{t} ");
		 }
		 
		 if(StringUtils.isNotBlank(waybillStatus)){
			 if(waybillStatus.equals("e")){
				 sb.append(" and (wb.waybill_status=-1 or wb.waybill_status=-2) ");
			 }else{
				 sb.append(" and wb.waybill_status=#{status} ");
			 }
		 }
		 
		 if(StringUtils.isNotBlank(waybillId)){
			 sb.append(" and wb.id=#{waybill_id} ");
		 }
		 
		 if(startTime!=null && endTime!=null){
			 sb.append(" and wb.create_time>="+startTime+" and wb.create_time<="+endTime+" ");
		 }
		 
		 sb.append(" order by wb.create_time desc");
		 System.out.println(sb);
		 return sb.toString();
	}	
	
	/**
	 * 查询运单数据(O2O运单分配系统)
	 * @param courierId
	 * @param waybillId
	 * @param waybillStatus
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object searchWaybillInfo(@SqlParameter("m")Map<String, String> pm,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		 boolean ismatchCourier = false;
		 String cnameSql = "";
		 String crealNamesql = "";
		 String cphonesql = "";
		 String courierName = pm.get("c_name");
		 String courierRealName = pm.get("c_realname");
		 String courierPhone = pm.get("c_phone");
		 if(StringUtils.isNotBlank(courierName)){
			 cnameSql = " and c.name=#{m.c_name} ";
			 ismatchCourier = true;
		 }
		 if(StringUtils.isNotBlank(courierRealName)){
			 pm.put("c_realname", "%"+pm.get("c_realname")+"%");
			 cnameSql = " and c.real_name like #{m.c_realname} ";
			 ismatchCourier = true;
		 }
		 if(StringUtils.isNotBlank(courierPhone)){
			 cphonesql = " and c.phone=#{m.c_phone} ";
			 ismatchCourier = true;
		 }
		 
		 
		 StringBuilder sb = new StringBuilder();
		 sb.append("select ");
		
		 sb.append(" wb.id id,wb.waybill_status status ,sp.name shipper_name,sp.address_x shipper_x,sp.address_y shipper_y,sp.address shipper_address,ifnull(sp.address_detail,'') shipper_address_detail,sp.phone shipper_phone, ");
		 sb.append(" u.name buyer_name,u.address buyer_address,u.address_x buyer_x,u.address_y buyer_y,u.phone buyer_phone,wb.payment_status payment_status,wb.pay_type pay_type, ");
		 sb.append(" wb.IS_PREPAY is_prepay,FROM_UNIXTIME((wb.create_time/1000),'%Y-%m-%d %T') create_time,wb.finish_time finish_time,wb.EXPECTED_FETCH_TIME fetch_time,wb.EXPECTED_ARRIVAL_TIME arrival_time");
		
		 //if(ismatchCourier){
			 sb.append(" ,c.name courier_name,c.real_name courier_realname,c.phone courier_phone ");
		// }
		 sb.append(" from waybill wb join shippers sp on sp.id=wb.shippers_ID left join user u on u.id = wb.buyer_id  ");
		 
		 //if(ismatchCourier){
			 sb.append(" left join courier c on wb.courier_id = c.id ");
		 //}
		 sb.append(" where 1=1 ");
		 
		 String thirdPlatFormId = pm.get("third_platform_id");
		 if(StringUtils.isNotBlank(thirdPlatFormId)){
			 sb.append(" and THIRD_PLATFORM_ID=#{m.third_platform_id} ");
		 }
		 
		 String waybillStatus = pm.get("status");
		 if(StringUtils.isNotBlank(waybillStatus)){
			 sb.append(" and wb.waybill_status=#{m.status} ");
		 }
		 
		 String waybillId = pm.get("id");
		 if(StringUtils.isNotBlank(waybillId)){
			 sb.append(" and wb.id=#{m.id} ");
		 }
		 

		 /*是否需要匹配配送员*/
		 if(StringUtils.isNotBlank(cnameSql)){
			 sb.append(cnameSql);
		 }
		 if(StringUtils.isNotBlank(crealNamesql)){
			 sb.append(crealNamesql);
		 }
		 if(StringUtils.isNotBlank(cphonesql)){
			 sb.append(cphonesql);
		 }
		 
		 
		 /*发货人坐标*/
		 String spx = pm.get("sp_x");
		 String spy = pm.get("sp_y");
		 if(StringUtils.isNotBlank(spx)&&StringUtils.isNotBlank(spy)){
			 sb.append(" and sp.address_x=#{m.sp_x} and sp.address_y=#{m.sp_y} ");
		 }
		 
		 /*收货人坐标*/
		 String bux = pm.get("bu_x");
		 String buy = pm.get("bu_y");
		 if(StringUtils.isNotBlank(bux)&&StringUtils.isNotBlank(buy)){
			 sb.append(" and u.address_x=#{m.bu_x} and u.address_y=#{m.bu_y} ");
		 }
		 
		 /*发货地址like匹配*/
		 String spAddress = pm.get("sp_address");
		 if(StringUtils.isNotBlank(spAddress)){
			 pm.put("sp_address", "%"+pm.get("sp_address")+"%");
			 sb.append(" sp.address like #{m.sp_address} ");
		 }
		 /*发货地址like匹配*/
		 String buAddress = pm.get("bu_address");
		 if(StringUtils.isNotBlank(buAddress)){
			 pm.put("bu_address", "%"+pm.get("bu_address")+"%");
			 sb.append(" u.address like #{m.bu_address} ");
		 }		 
		 sb.append(" order by wb.create_time desc");
		 
		 return sb.toString();
	}	
	
	
	/**
	 * 根据订单ID更新订单状态
	 * @deprecated 遗留接口，新的接口参考 updateWaybillById
	 * @param ksbWaybillId
	 * @param status
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateWaybillStatusByKSBId(@SqlParameter("id") Long ksbWaybillId,@SqlParameter("status") String status){
		
		StringBuilder sb =new StringBuilder("update waybill set waybill_status=#{status} where id=#{id}");
		
		return sb.toString();
	}
	
	/**
	 * 根据订单ID更新支付结果(仅仅更新运单支付状态)
	 * @param ksbWaybillId
	 * @param status
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateWaybillPayStatusByKSBId(@SqlParameter("id") Long ksbWaybillId,@SqlParameter("status") String status){
		
		StringBuilder sb =new StringBuilder("update waybill set payment_status=#{status} where id=#{id}");
		
		return sb.toString();
	}
	
	/**
	 * 为运单增加一条配送历史记录
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object addWaybillPSRecord(@SqlParameter("m") Map<String, Object> map){
		
		map.put("operate_time", new Date().getTime());
		StringBuilder sb = new StringBuilder("insert into waybill_distribution_record(");
		sb.append("id,OPERATE_TIME,waybill_id,OPERATOR,action_description");
		sb.append(") values(");
		sb.append("#{"+ReservedWord.snowflake+"},#{m.operate_time},#{m.waybill_id},#{m.operator},#{m.action_description}");
		sb.append(")");
		
		return sb.toString();
	}
	
	/**
	 * 查询指定运单的配送历史记录(打算在O2O的时候启用)
	 * @param waybillId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select
	public Object queryWaybillPSRecord(@SqlParameter("id") String waybillId){
		
		StringBuilder sb = new StringBuilder();
		sb.append("select ");
		sb.append(" id,OPERATE_TIME,waybill_id,OPERATOR,action_description ");
		sb.append(" from waybill_distribution_record where waybill_id=#{id} order by OPERATE_TIME asc");
		return sb.toString();
	}
	
	/**
	 * 查询电商运单(仅查询非 异常的运单,主要应用在电商运单扫描)
	 * @param olOrderId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean,resultType=WayBillEntity.class)	
	public Object queryOlWaybillById(@SqlParameter("oid") String olOrderId,@SqlParameter("kid") String ksbId){
		StringBuilder sb = new StringBuilder(" select wb.id,wb.shipper_origin_id,wb.waybill_status,wb.courier_id,wb.SHIPPER_ORIGIN_ID,sp.name,wb.is_topay,ifnull(wb.cargo_price,'0') cargo_price,ifnull(wb.FETCH_BUYER_FEE,'0') fetch_buyer_fee,ifnull(wb.remarks,'') remarks ");
		sb.append(" from waybill wb join shippers sp on wb.shippers_id = sp.id where 1=1 and waybill_status>=0 ");
		
		if(StringUtils.isNotBlank(olOrderId)){
			sb.append(" and wb.SHIPPER_ORIGIN_ID=#{oid} ");
		}
		if(StringUtils.isNotBlank(ksbId)){
			sb.append(" and wb.id=#{kid} ");
		}
		
		/*电商运单，配送多次，数据库中 一个电商运单ID 可能会出现多次(默认显示最近一次)*/
		sb.append(" order by wb.create_time desc limit 1 ");
		
		return sb.toString();
	}
	
	/**
	 * 统计配送员当前运单情况
	 * @param courierId
	 * @param startTime
	 * @param endTime
	 * @param waybillType
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.mapList)	
	public Object currentDayWaybillStatByCourier(@SqlParameter("cid") String courierId,@SqlParameter("st")long startTime,@SqlParameter("et")long endTime,@SqlParameter("wt")String waybillType){
		
		StringBuilder sb = new StringBuilder();
		sb.append("select waybill_status status,count(id) num from waybill where 1=1 and waybill_type=#{wt} ");
	    
		sb.append(" and create_time>=#{st} and create_time<=#{et} ");
		
		if(StringUtils.isNotBlank(courierId)){
	    	sb.append("and courier_id =#{cid} ");
	    }
		
		sb.append(" group by waybill_status ");
		
		return sb.toString();
	}

	/**
	 * 统计商家当前运单情况
	 * @param courierId
	 * @param startTime
	 * @param endTime
	 * @param waybillType
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.mapList)	
	public Object currentDayWaybillStatByShipper(@SqlParameter("sp_id") String shipperId,@SqlParameter("st")long startTime,@SqlParameter("et")long endTime,@SqlParameter("wt")String waybillType){
		
		StringBuilder sb = new StringBuilder();
		sb.append("select waybill_status status,count(id) num from waybill where 1=1 and waybill_type=#{wt} ");
	    
		sb.append(" and create_time>=#{st} and create_time<=#{et} ");
		
		if(StringUtils.isNotBlank(shipperId)){
	    	sb.append("and shippers_id =#{sp_id} ");
	    }
		
		sb.append(" group by waybill_status ");
		
		return sb.toString();
	}
	
	
	
	/**
	 * 更新运单信息(接口主要应用在 运单状态变更，关联的部分属性联动变更)
	 * @param wb
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateWaybillById(@SqlParameter("entity") WayBillEntity wb){
		
		StringBuilder sb = new StringBuilder("update waybill set id=id ");
		
		/*可修改的字段：
		 * waybill_status
		 * payment_status
		 * waybill_fee
		 * pay_shipper_fee
		 * cargo_price
		 * fetch_buyer_fee
		 * finish_time
		 * remarks
		 * */
		
		if(StringUtils.isNotBlank(wb.getWaybill_status())){
			sb.append(" ,waybill_status=#{entity.waybill_status} ");
		}
		
		if(StringUtils.isNotBlank(wb.getPayment_status())){
			sb.append(" ,payment_status=#{entity.payment_status} ");
		}
		
		if(StringUtils.isNotBlank(wb.getWaybill_fee())){
			sb.append(" ,waybill_fee=#{entity.waybill_fee} ");
		}
		
		if(StringUtils.isNotBlank(wb.getPay_shipper_fee())){
			sb.append(" ,pay_shipper_fee=#{entity.pay_shipper_fee} ");
		}
		
		if(StringUtils.isNotBlank(wb.getCargo_price())){
			sb.append(" ,cargo_price=#{entity.cargo_price} ");
		}
		
		if(StringUtils.isNotBlank(wb.getFetch_buyer_fee())){
			sb.append(" ,fetch_buyer_fee=#{entity.fetch_buyer_fee} ");
		}
		
		if(StringUtils.isNotBlank(wb.getRemarks())){
			sb.append(" ,remarks=#{entity.remarks} ");
		}
		
		if(StringUtils.isNotBlank(wb.getFinish_time())){
			sb.append(" ,finish_time=(UNIX_TIMESTAMP() * 1000)");
		}
		
		sb.append(" where id=#{entity.id} ");
		
		if(StringUtils.isNotBlank(wb.getCourier_id())){
			sb.append(" and courier_id=#{entity.courier_id} ");
		}
		
		return sb.toString();
	}
	
	
	/**
	 * 批量收取O2O的运单(批量收件)
	 * @param wb
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object batchFetchO2OWaybill(@SqlParameter("cid")String cid,@SqlParameter("list") List<String> waybillIdList){
		
		StringBuilder sb = new StringBuilder("update waybill set waybill_status=3 ");
		
		sb.append(" where id=#{list."+ReservedWord.index+"} and courier_id=#{cid} ");
		
		return sb.toString();
	}
	
	/**
	 * 批量把运单分配给配送员
	 * @param courierId
	 * @param status
	 * @param waybillList
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object batchAllocateWaybill2Courier(@SqlParameter("cid") String courierId,@SqlParameter("status")String status,@SqlParameter("list") List<String> waybillList){
		
		StringBuilder sb = new StringBuilder("update waybill set courier_id=#{cid},waybill_status=#{status} where id=#{list."+ReservedWord.index+"}");
		return sb.toString();
	}
	
	/**
	 * 
	 * @param cid
	 * @param courierId
	 * @param startTime
	 * @param endTime
	 * @param waybillType
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType = CollectionType.map)
	public Object courierPaymentStat(@SqlParameter("cid") String cid,@SqlParameter("st")long startTime,@SqlParameter("et")long endTime,@SqlParameter("t") String waybillType){
		
		StringBuffer sb = new StringBuffer("");
		
		sb.append(" select count(id) total_waybill,ifnull(sum(pay_shipper_fee),0) total_shipper_fees,ifnull(sum(fetch_buyer_fee),0) total_buyer_fees from waybill where 1=1 ");
		
		if(StringUtils.isNotBlank(waybillType)){
			sb.append(" and waybill_type=#{t}  ");
		}
		if(StringUtils.isNotBlank(cid)){
			sb.append(" and courier_id=#{cid} ");
		}
		
		sb.append(" and create_time>=#{st} and create_time<=#{et} ");
		return sb.toString();
	}
	
	/**
	 * 配送员添加自定义备注信息
	 * @param courierId
	 * @param waybillId
	 * @param customRemarks
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateWaybillCustomRemarks(@SqlParameter("cid") String courierId,@SqlParameter("id")String waybillId,@SqlParameter("cm") String customRemarks){
		StringBuilder sb = new StringBuilder("update waybill set custom_remarks=#{cm} where id=#{id} and courier_id=#{cid}");
		return sb.toString();
	}

	
	/**
	 * 商家更新订单状态(一般操作多为取消订单)
	 * @param spId
	 * @param spUid
	 * @param waybillId
	 * @param status
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update	
	public Object updateWaybillStatusByShipper(@SqlParameter("sp_id") String spId,@SqlParameter("sp_uid") String spUid,@SqlParameter("id") String waybillId,@SqlParameter("status") String status){
		
		StringBuilder sb = new StringBuilder("update waybill set waybill_status=#{status} where id=#{id} and shippers_id=#{sp_id}");
		return sb.toString();
		
	}
	
	/**
	 * 未分配给配送员的订单
	 * @param waybillId
	 * @param cityCode
	 * @param x
	 * @param y
	 * @param eids
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert	
	public Object saveUnallocateWaybill(@SqlParameter("waybill_id") String waybillId,@SqlParameter("city_code") String cityCode,@SqlParameter("x") String x,@SqlParameter("y") String y,@SqlParameter("eids") String eids){
		
		StringBuilder sb = new StringBuilder("insert into unallocate_waybill(id,waybill_id,create_time,city_code,sp_x,sp_y,delivery_eids) ");
		
		sb.append("values(#{"+ReservedWord.snowflake+"},#{waybill_id},(UNIX_TIMESTAMP() * 1000),#{city_code},#{x},#{y},#{eids})");
		return sb.toString();
	}
	
}
