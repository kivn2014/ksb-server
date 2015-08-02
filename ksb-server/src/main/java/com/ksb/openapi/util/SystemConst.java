package com.ksb.openapi.util;

public class SystemConst {


	/*redis缓存中 key值（主要解决一个商家 一次性导入多个运单）*/
	public static final String KSB_WAYBILL_ONE2MORE_GEO_KEY = "ksb.waybill.one2more.geo.key";
	
	/*优化结果分组中，每个点相互间距最大公里数*/
	public static final String KSB_WAYBILL_GROUP_DISTANCE = "ksb.waybill.group.distance";
	
	/*一个优化分组单元，每个分组中最多元素个数*/
	public static final String KSB_WAYBILL_GROUP_MAXNUM = "ksb.waybill.group.maxnum";
	
	/*运单合并优化 搜索半径*/
	public static final String KSB_WAYBILL_BASE_RADIUS = "ksb.waybill.search.base.radius";
	
	/*闪送host地址*/
	public static final String SHANSONG_HOST = "shansong.host";
	
	/*达达rest api host地址*/
	public static final String DADA_RESTAPI_HOST = "dada.restapi.host";
	
	public static final String DADA_CALLBACK_URL = "dada.restapi.callback.url";
	
	/*是否启用redis*/
	public static final String REDIS_ENABLE = "redis.enable";
	
	/*订单配送，默认查询的范围*/
	public static final String COURIER_QUERY_RADIUS = "near.courier.radius";
	
	/*配送员开工收工状态前缀*/
	public static final String COURIER_WORKSTATUS_PREFIX = "redis.workstatus.prefix";
	
	/*配送员O2O订单配送状态前缀*/
	public static final String COURIER_PS_STATUS_PREFIX = "redis.ps.status.prefix";
	
	/*存储配送员最近一次上报的位置*/
	public static final String COURIER_GEO_PREFIX = "redis.courier.geo.prefix";
	
	/*未分配的订单*/
	public static final String UNALLOCATE_PREFIX = "redis.unallocate.prefix";
	
}
