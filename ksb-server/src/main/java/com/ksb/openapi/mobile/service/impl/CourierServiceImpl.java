package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.property.PropertiesConfigUtil;
import autonavi.online.framework.sharding.dao.DaoHelper;

import com.esotericsoftware.minlog.Log;
import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.entity.CourierEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.CourierService;
import com.ksb.openapi.util.DesUtil;
import com.ksb.openapi.util.MD5Util;
import com.ksb.openapi.util.SystemConst;

@Service
public class CourierServiceImpl implements CourierService {


	@Autowired
	UserDao userDao = null;
	
	private Logger log = LogManager.getLogger(getClass());
	
	@Override
	public ResultEntity authen(String userName, String password,String eid) {
		// TODO Auto-generated method stub
		
		ResultEntity rsEntity = new ResultEntity();
		rsEntity.setSuccess(false);
		CourierEntity courierEntity = null;
		try{
			password = MD5Util.MD5(MD5Util.MD5(password));
			
			courierEntity = (CourierEntity)userDao.authenCourier(userName, password);
		}catch(Exception e){
			Log.error("快递员登录接口系统异常",e);
			rsEntity.setErrors("ER");
			rsEntity.setObj("系统异常,登录失败");
			return rsEntity;
		}
		
		if(courierEntity==null){
			rsEntity.setErrors("ER");
			rsEntity.setObj("用户名或者密码不对");
			return rsEntity;
		}
		
		String status = courierEntity.getStatus();
		/*如果没有获取到状态字段，则无法登陆*/
		if(StringUtils.isBlank(status)){
			rsEntity.setErrors("ER");
			rsEntity.setObj("账号被停用");
			return rsEntity;
		}
		
		/*状态非0 表示异常*/
		if(!status.equals("0")){
			rsEntity.setErrors("ER");
			rsEntity.setObj("账号被锁定");
			return rsEntity;
		}
		
		rsEntity.setSuccess(true);
		rsEntity.setErrors("OK");
		rsEntity.setObj(courierEntity);
		return rsEntity;
	}

	@Override
	public ResultEntity updateCourierPasswd(String eid,String userName, String oldPasswd,
			String newPasswd) {
		if(StringUtils.isBlank(eid)){
			eid = "100";
		}
		/*原始密码,使用自定义的可逆加密算法存储*/
		String srcPwd = newPasswd;
		try {
			srcPwd = DesUtil.getInstance().encrypt(newPasswd);
		} catch (Exception e1) {}
		
		/*密码使用两次MD5加密*/
		String op = MD5Util.MD5(MD5Util.MD5(oldPasswd));
		String np = MD5Util.MD5(MD5Util.MD5(newPasswd));
		
		CourierEntity courierEntity = null;
		
		ResultEntity rsEntity = new ResultEntity();
		rsEntity.setSuccess(false);
		try {
			courierEntity = (CourierEntity)userDao.authenCourier(userName, op);
			if(courierEntity==null){
				rsEntity.setErrors("ER");
				rsEntity.setObj("旧密码不正确");
				return rsEntity;
			}
			userDao.updateCourierPasswd(null, userName, op, np, srcPwd);
		} catch (Exception e) {
			rsEntity.setErrors("ER");
			rsEntity.setObj("系统异常");
			return rsEntity;
		}
		
		rsEntity.setSuccess(true);
		rsEntity.setErrors("OK");
		return rsEntity;
	}

	
	@Override
	public CourierEntity queryFreeCourier(String uid, String realName,String phone) {

		CourierEntity courierEntity = null;
		try {
			if(StringUtils.isNotBlank(realName)){
				realName = "%"+realName+"%";
			}
			
			courierEntity = (CourierEntity) userDao.queryFreeCourierInfo(uid,realName,phone);
		} catch (Exception e) {
			Log.error("查询时系统异常", e);
			return null;
		}

		return courierEntity;
	}

	@Override
	public CourierEntity queryCourier(String uid, String realName,String phone) {

		CourierEntity courierEntity = null;
		try {
			if(StringUtils.isNotBlank(realName)){
				realName = "%"+realName+"%";
			}
			
			courierEntity = (CourierEntity) userDao.queryFreeCourierInfo(uid,realName,phone);
		} catch (Exception e) {
			Log.error("查询时系统异常", e);
			return null;
		}

		return courierEntity;
	}	
	
	@Override
	public Map<String, Object> queryCorierList(CourierEntity entity,int skip,int size){
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		List<CourierEntity> rsList = null;
		long totalNum = 0;
		try{
			rsList = (List<CourierEntity>)userDao.queryCourierInList(entity, skip, size);
			totalNum = DaoHelper.getCount();
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		rsMap.put("1", rsList);
		rsMap.put("2", totalNum);
		return rsMap;
	}
	
	
	void handle(List<CourierEntity> rsList){
//		for(){
//			
//		}
		
		
	}
	
	
	
	@Override
	public void createCourier(CourierEntity courierEntity) {

		String pwd = courierEntity.getPwd();
		
		/*原始密码,使用自定义的可逆加密算法存储*/
		String srcPwd = pwd;
		try {
			srcPwd = DesUtil.getInstance().encrypt(pwd);
		} catch (Exception e1) {}
		/*密码使用两次MD5加密*/
		pwd = MD5Util.MD5(MD5Util.MD5(pwd));
		courierEntity.setPwd(pwd);
		courierEntity.setSrcPwd(srcPwd);

		try{
		   userDao.createCourier(courierEntity);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}

	public void batchCreateCourier(List<CourierEntity> list){
		
		try{
			userDao.batchCreateCourier(list);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
	}
	
	
	
	@Override
	public void updateCourierWorkStatus(String cid,String eid, String workStatus,String x,String y) {
		
		log.entry(cid,eid,workStatus,x,y);
		try{
			userDao.updateCourierWorkStatus(cid, workStatus,x,y);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new BaseSupportException(e);
		}
	}

	@Override
	public void recordCourierGps(String cid, String eid, String x, String y) {
		log.entry(cid,eid,x,y);
		try{
			log.entry("courier_gps_2db",cid, x, y);
		    userDao.updateCourierGps(cid, x, y);
		}catch(Exception e){}
	}

	@Override
	public List<String> queryNearCourier(String eid,String x,String y){
		
		log.entry(eid,x,y);
		
		List<String> courierList = new ArrayList<String>();
		
		/*查找半径(半径为km)*/
		String radiusStr = "3";
		try {
			radiusStr = (String)PropertiesConfigUtil.getPropertiesConfigInstance().getProperty(SystemConst.COURIER_QUERY_RADIUS);
		} catch (Exception e) {}
		
		/*从数据库中获取最近的配送员*/
		log.entry("userDao.queryNearCourier",eid,x, y, radiusStr);
		
		/*数据库查询distance距离单位为 米*/
		List<Map<String, String>> dbList = (List<Map<String, String>>)userDao.queryNearCourier(eid,x, y, Integer.parseInt(radiusStr)*1000);
		log.entry("数据库中查到配送公司["+eid+"];半径["+radiusStr+"]公里内的配送员 ",dbList);
		if(dbList==null||dbList.size()==0){
			return courierList;
		}
		
		Map<String, String> dbMap = dbList.get(0);
			
		courierList.add(String.valueOf(dbMap.get("id"))+"^"+String.valueOf(dbMap.get("enterprise_id")));

		return log.exit(courierList);
	}

	
}
