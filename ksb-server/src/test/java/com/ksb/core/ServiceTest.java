package com.ksb.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.util.internal.DetectionUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import autonavi.online.framework.sharding.dao.DaoHelper;

import com.ksb.openapi.dao.EnterpriseDao;
import com.ksb.openapi.dao.ProductVersionDao;
import com.ksb.openapi.dao.StatisticsDao;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.em.ProductType;
import com.ksb.openapi.em.WaybillType;
import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.CourierEntity;
import com.ksb.openapi.entity.EnterpriseEntity;
import com.ksb.openapi.entity.ProductVersionEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.ShipperUserEntity;
import com.ksb.openapi.entity.WayBillEntity;
import com.ksb.openapi.mobile.service.CourierService;
import com.ksb.openapi.mobile.service.EretailerService;
import com.ksb.openapi.mobile.service.O2oWaybillService;
import com.ksb.openapi.mobile.service.RedisService;
import com.ksb.openapi.mobile.service.ShipperService;
import com.ksb.openapi.mobile.service.impl.CourierServiceImpl;
import com.ksb.openapi.service.StatisticsService;
import com.ksb.openapi.service.WaybillService;
import com.ksb.openapi.util.DateUtil;
import com.ksb.openapi.util.JedisUtil;
import com.ksb.openapi.util.MD5Util;
import com.ksb.web.service.WebWaybillService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
		"classpath:ksb/applicationContext.xml" })
public class ServiceTest {

	@Autowired
	WaybillDao waybillDao;
	
	@Autowired
	EnterpriseDao enterpriseDao;
	
	@Autowired
	UserDao userDao;
	
	@Autowired
	CourierService courierService;

	@Autowired
	EretailerService eretailerService;
	
	@Autowired
	O2oWaybillService o2oWaybillService;
	
	@Autowired
	WebWaybillService webWaybillService;
	
	@Autowired
	WaybillService waybillService;
	
	@Autowired
	ShipperService shipperService;
	
	@Autowired
	ProductVersionDao productVersionDao;
	
	@Autowired
	StatisticsDao statDao;
	
	@Autowired
	StatisticsService statisticsService;
	
	@Autowired
	RedisService redisService;
	
	@Test
	public void start() throws Exception {
		
		
//		while (true){
//		}
		
		
//		redisService.
		
		
		//createCourier();
		//queryCourierById();
//		authen();
//		updatePassword();
		//queryShipperList();
		//olWaybillSave();
	    //statCourierWaybill();
		//updateWaybill();
		//scanorder();
//		saveOlOrder();
//		queryCourierTodayWaybill();
		
//		queryO2OWaybillList();
//		statO2Owaybill();
		//batchFetchWaybill();
//		batchAllocate();
		
//		saveWaybill();
		//countWaybill();
//		courierO2OCount();
//		updateCourierPsStatus();
//		searchShipperAddressList();
//		shipperUser();
		//updateSpAddress();
		//createShipper();
		
		//shipperO2oWaybillList();
		//searchNearCourier();
		
		
		//updateWorkStatus();
		
		
		//spCreateWaybill();
		//querySpUser();
//		createSpInfo();
		//testRedis();
		
//		System.out.println(JedisUtil.getInstance().STRINGS.("test-key"));
		
//		searchCourier();
		
//		addAppVersion();
		
//		queryAppLastVersion();
		//createPsEnt();
		
//		createSpEnt();
		
		//statDateStatus();
		
		//statDateStatusCourier();
		
		//shipperCancelWaybill();
		//saveUnallocate();
		//searchWaybillTemp();
		
		batchSaveCourier();
	}
	
	
	public void batchSaveCourier(){
		
		List<CourierEntity> list = new ArrayList<CourierEntity>();
		
		CourierEntity e = new CourierEntity();
		e.setDelivery_status("1");
		e.setWork_status("2");
		e.setReal_name("测试1");
		e.setName("test1");
		e.setEnterprise_id("100");
		e.setPhone("991");
		e.setPwd("991");
		
		CourierEntity e1 = new CourierEntity();
		e1.setDelivery_status("1");
		e1.setWork_status("2");
		e1.setReal_name("测试1");
		e1.setName("test1");
		e1.setEnterprise_id("100");
		e1.setPhone("991");
		e1.setPwd("991");
		
		list.add(e);
		list.add(e1);
		
		courierService.batchCreateCourier(list);	
		
		System.out.println("=====");
	}
	
	
	public void searchWaybillTemp(){
		waybillDao.searchWaybillInfo(new HashMap<String, String>(), 0, 10);
	}
	
	public void saveUnallocate(){
		
		waybillDao.saveUnallocateWaybill("3km-612905826163621888", "010", "116.391234", "39.4678", "100");
		
		
	}
	
	
	public void shipperCancelWaybill(){
		
		shipperService.shipperHandleWaybill("612905826163621888", "", "3km-1", "-3");
		
	}
	
	
	public void statDateStatus(){
		
		//List<Map<String, String>> rsList = (List<Map<String, String>>)statDao.groupQueryDateStatus("100", DateUtil.convertStringToLong("2015-07-20"), new Date().getTime());
//		
//		Map<String, Map<String, String>> rsMap = statisticsService.groupQueryDateStatus("100", DateUtil.convertStringToLong("2015-07-20"), new Date().getTime());
//		
//		System.out.println(rsMap);
		
	}
	
	public void statDateStatusCourier(){
		
//		List<Map<String, String>> rsList = (List<Map<String, String>>)statDao.queryCourierStatusByDate("100", "%宋%", "2015-07-20");
		
//		Map<String, Map<String, String>> rsMap = statisticsService.groupQueryCourierStatusByDate("100", "", "2015-07-23",0,10);
//		
//		System.out.println(rsMap);
		
	}
	
	public void createPsEnt(){
		
		EnterpriseEntity ent = new EnterpriseEntity();
		ent.setCity("北京市");
		ent.setProvince("北京市");
		ent.setName("平安集团");
		ent.setTel("010-9989877");
		ent.setStatus("2");
		
		enterpriseDao.createEnterprise(ent);
	}
	
	public void createSpEnt(){
		
		ShipperEntity ent = new ShipperEntity();
		ent.setCity("北京市");
		ent.setProvince("北京市");
		ent.setName("平安集团");
		ent.setTel("010-9989877");
		ent.setStatus("2");
		
		enterpriseDao.createShipper(ent, "100");
		
	}	
	
	
	
	public void queryAppLastVersion(){
		
		ProductVersionEntity entity = (ProductVersionEntity)productVersionDao.queryAppLatestVersion(ProductType.SP.getName());
		System.out.println(entity.getVersion_code()+"===="+entity.getPublish_time());
	}
	
	public void addAppVersion(){
		
		
		/*增加商家版app版本*/
		ProductVersionEntity entity = new ProductVersionEntity();
		entity.setVersion_num(5);
		entity.setVersion_code("V5.0.1");
		entity.setProduct_type(ProductType.SP.getName());
	    productVersionDao.createProductVersion(entity);
		
		
		/*增加配送员版app版本*/
		entity.setProduct_type(ProductType.COURIER.getName());
	    productVersionDao.createProductVersion(entity);
		
	}
	
	
	public void searchCourier(){
		
		CourierEntity e = new CourierEntity();
		e.setEnterprise_id("500");
		
		List<Map<String, String>> list = (List<Map<String,String>>)userDao.queryCourierInList(e, 0, 10);
		System.out.println(list);
		
		
	}
	
	public void testRedis(){
		
		for(int i=0;i<200;i++){
			try{
				System.out.println("get_ent_from_redist-redis_city_area_100");
				
				Set<String> entSet = JedisUtil.getInstance().SETS.smembers("redis_city_area_010");
				System.out.println("get_ent_from_redist_result "+entSet);
			}catch(Exception e){
				e.printStackTrace();
			}

		}
		
	}
	
	
	
	public void createSpInfo(){
		
		
		ShipperUserEntity entity = new ShipperUserEntity();
		entity.setSp_name("asdfasd");
		entity.setName("sdasfdasdf");
		entity.setPasswd("asdfasdfas");
		entity.setPhone("asdfasdfasd");
		
		shipperService.createShipperAndUser(entity);
		
		
	}
	
	
	public void querySpUser(){
		
		shipperService.querySpUser("500", "", 0, 10);
		
		
	}
	
	
	public void spCreateWaybill(){
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("sp_id", "100");
		map.put("city_name", "北京市");
		map.put("x", "116.311664");
		map.put("y", "39.990903");
		shipperService.createWaybill(map);
		
	}
	
	
	public void updateWorkStatus(){
		
		//courierService.updateCourierWorkStatus("608224890197114880", "100", "1", "116.311664", "39.990903");
		
		
	}
	
	public void searchNearCourier(){
		
//		List<Map<String, String>> list = (List<Map<String,String>>)userDao.queryNearCourier("116.311764", "39.992932", 10000);
//		System.out.println(list);
	}
	
	
	public void shipperO2oWaybillList(){
		
		List<Map<String, String>> list = (List<Map<String,String>>)waybillDao.queryShipperO2OWaybillList("615033426856116224", null, null, WaybillType.O2O.getName(), null, null,0, 10);
		
		System.out.println(list);
		
	}
	
	public void createShipper(){
		
		ShipperUserEntity e = new ShipperUserEntity();
		e.setSp_name("东来顺");
		e.setName("kevi");
		e.setReal_name("侯世鹏");
		e.setPasswd("123456");
		e.setPhone("1340999999");
		e.setCity_name("北京市");
		e.setAddress("大恒科技大厦南座18层");
		
		shipperService.createShipperAndUser(e);
		//eruserDao.createShipperUser(e);
	}
	
	
	public void updateSpAddress(){
		
		String address = "北京市海淀区稻香园";
		String addressDetail = "2号楼1-1001";
		String x = "0";
		String y = "0";
		
		//enterpriseDao.updateShipperAddress("1",null, address, addressDetail, x, y);
		System.out.println("====");
		
	}
	
	
	public void shipperUser(){
		
		
//		ShipperUserEntity spe = shipperService.authenSpUser("kk", "123456");
//		System.out.println(spe.getName()+"---------"+spe.getAddress());
	}
	
	
	public void updateCourierPsStatus(){
		
		userDao.updateCourierPsStatus("608224890197114880", "1");
	}
	
	public void searchShipperAddressList(){
		
		List<Map<String, String>> list = (List<Map<String, String>>)enterpriseDao.queryShipperAddress("1",null);
		System.out.println(list);
	}
	
	public void courierO2OCount(){
		
		Map<String, String> map = o2oWaybillService.courierPaymentStat("608224890197114880");
		System.out.println(map);
	}

	public void countWaybill(){
//		eretailerService.currentDayWayBillStatistic("608224890197114880");
		
		o2oWaybillService.currentDayWayBillStatistic("608224890197114880");
		
	}
	
	public void saveWaybill(){
		
//{shipper_name=kevi_123, booking_fetch=0, city_name=北京市, booking_fetch_time= , shipper_address_y=39.988054, buyers_list=[{name: "Mouy", phone: "444445555", address_x: 116.310648, address_y: 39.990476, address: "%E5%B7%A6%E5%B2%B8%E5%85%AC%E7%A4%BE%EF%BC%88%E6%B5%B7%E6%B7%80%E5%8C%BA%E5%8C%97%E5%9B%9B%E7%8E%AF%E8%A5%BF%E8%B7%AF68%E5%8F%B7%EF%BC%89706"}], 
		//shipper_address=大恒科技大厦（北京市海淀区苏州街3号）南座18层, cargo_weight=1, shipper_address_x=116.311906, shipper_phone=1111, remarks=, cargo_price=0, goodsName=资料}
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("shipper_name", "kevi_123");
		map.put("booking_fetch", "0");
		map.put("city_name", "北京市");
		map.put("booking_fetch_time", " ");
		map.put("shipper_address_y", "39.988054");
		map.put("buyers_list", "[{name: \"Mouy\", phone: \"444445555\", address_x: 116.310648, address_y: 39.990476, address: \"%E5%B7%A6%E5%B2%B8%E5%85%AC%E7%A4%BE%EF%BC%88%E6%B5%B7%E6%B7%80%E5%8C%BA%E5%8C%97%E5%9B%9B%E7%8E%AF%E8%A5%BF%E8%B7%AF68%E5%8F%B7%EF%BC%89706\"}]");
		map.put("shipper_address", "大恒科技大厦（北京市海淀区苏州街3号）南座18层");
		map.put("cargo_weight", "1");
		map.put("shipper_address_x", "116.311906");
		map.put("shipper_phone", "1111");
		map.put("cargo_price", "0");
		
		
		
		waybillService.addWaybillByOpenApi(map);
		
		
	}
	
	
	public void batchAllocate(){
		List<String> list = new ArrayList<String>();
		list.add("3km_601335030131720192");
		webWaybillService.batchAllocateWaybill2Courier("608224890197114880", list);
		
	}
	
	
	public void queryCourierList(){
		
		CourierEntity entity = new CourierEntity();
		entity.setEnterprise_id("100");
		//List<CourierEntity> list = (List<CourierEntity>)webWaybillService.queryCorierList(entity, 0, 10);
		//System.out.println(list);
	}
	
	public void batchFetchWaybill(){
		
		//608937397479931904 608934457457377280
		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("cid", "608224890197114880");
		paraMap.put("handle_list", "[{\"id\":\"608937397479931904\",\"status\":3},{\"id\":\"608934457457377280\",\"status\":3}]");
		
		o2oWaybillService.batchFetchO2OWaybill(paraMap);
		
	}
	
	public void statO2Owaybill(){
		
		List<Map<String, String>> list = o2oWaybillService.currentDayWayBillStatistic("608224890197114880");
		System.out.println("--------------------------------------->>>"+list);
	}
	
	
	public void queryO2OWaybillList(){
		
//		List<Map<String, String>> list = (List<Map<String, String>>)waybillDao.queryCourierTodayO2OWaybillList("608224890197114880", null, "", "", 0, 10);
		
		Map<String, Object> map = o2oWaybillService.queryWaybillByCourier("608224890197114880", null, null, 0, 10);
		System.out.println(map);
		
		
	}
	
	
	
	public void queryCourierTodayWaybill(){
		
//		List<Map<String, String>> list = (List<Map<String, String>>)waybillDao.queryCourierTodayOLWaybillList("608224890197114880", null, "1", "1", 0, 10);
//		System.out.println(list);

		Map<String, Object> map = o2oWaybillService.queryWaybillByCourier("608224890197114880", null, "", 0, 10);
		System.out.println(map);
		
	}
	
	
	public void saveOlOrder(){
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("cid", "608224890197114880");
		map.put("oid", "YMS88888");
		map.put("is_topay", "0");
		
		eretailerService.createOlWaybill(map);
		
		
	}
	
	
	public void scanorder(){
		
		String cid = "608224890197114880";
		String oid = "00000";
		
		eretailerService.scanOlWaybill(cid, oid);
		
	}
	
	public void updateWaybill(){
		
		WayBillEntity entity = new WayBillEntity();
		
		entity.setId("608500467026100224");
		entity.setFetch_buyer_fee("100000");
		entity.setWaybill_status("5");
		
		waybillDao.updateWaybillById(entity);
	}
	
//	public void statCourierWaybill(){
//		
////		List<Map<String, String>> map = (List<Map<String, String>>) waybillDao.currentDayWaybillStatByCourier("", 0, 1433951999000L);
////		
////		System.out.println(map);
////		for(Map<String, String> m :map){
////			System.out.println(m);
////		}
//		
//		
//		List<Map<String, String>> list = oLRetailerService.currentDayWayBillStatistic("");
//		System.out.println(list);
//		
//	}
//	
//	
//	public void olWaybillSave(){
//		
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("id", "JD09888888");
//		map.put("is_topay", "1");
//		map.put("fee", "112.90");
//		map.put("remarks", "晚上九点之前要送到");
//		
//		try{
//		oLRetailerService.createOlWaybill(map);
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//	}
//	
//	
//	public void queryShipperList(){
//		
//		Map<String, String> map = new HashMap<String, String>();
//		map.put("name","东1");
//		
//		List<ShipperEntity> list = shipperService.queryShipperList(map);
//		
//		System.out.println(list);
//		
//		
//	}
//	
//	
//	public void createCourier(){
//		
//		CourierEntity entity = new CourierEntity();
//		
//		entity.setEnterprise_id("100");
//		entity.setName("kevi_me");
//		entity.setReal_name("侯世鹏");
//		entity.setPhone("13999999");
//		entity.setPwd("123456");
//		
//		courierService.createCourier(entity);
//		
//		//userDao.createCourier(entity);
//	}
//	
//	
//	public void queryCourierById(){
//		
////		CourierEntity ce = (CourierEntity)userDao.queryCourierInfo("", "13999999");
//		CourierEntity ce = courierService.queryCourier("", "13999999");
//		
//		System.out.println(ce.getName());
//		
//		CourierEntity ce1 = courierService.queryCourier("", "88888");
//		
//		System.out.println(ce1.getName());
//	}
//	
//	
//	public void authen(){
//		
//		ResultEntity ce = courierService.authen("kevi_me", "123456",null);
//		System.out.println(ce);
//		ResultEntity ce1 = courierService.authen("kevi_me_1", "123456",null);
//		System.out.println(ce1);
//		ResultEntity ce3 = courierService.authen("kevi_me", "1234566",null);
//		System.out.println(ce3);
//	}
//	
//	public void updatePassword(){
//		
//		courierService.updateCourierPasswd(null, "kevi_me", "123456", "7890");
//		
//		
//	}
//	
	
	
}
