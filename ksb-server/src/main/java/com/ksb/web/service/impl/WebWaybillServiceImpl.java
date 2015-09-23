package com.ksb.web.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.sharding.dao.DaoHelper;

import com.ksb.openapi.dao.UserDao;
import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.entity.CourierEntity;
import com.ksb.openapi.entity.ResultEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.web.service.WebWaybillService;

@Service
public class WebWaybillServiceImpl implements WebWaybillService {

	@Autowired
	WaybillDao waybillDao;
	
	@Autowired
	UserDao userDao;
	
	@Override
	public Map<String, Object> searchWaybillInfo(Map<String, String> pm,int skip, int size) {
		// TODO Auto-generated method stub
		
		List<Map<String, String>>rsList = null; 
		long totalNum = 0;
		try{
			rsList = (List<Map<String, String>>)waybillDao.searchWaybillInfo(pm, skip, size);
			totalNum = DaoHelper.getCount();
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		rsMap.put("1", rsList);
		rsMap.put("2", totalNum);
		
		return rsMap;
	}
	

	
	@Override
	public void batchAllocateWaybill2Courier(String cid,List<String> waybillList){
		
		if(StringUtils.isBlank(cid)){
			throw new BaseSupportException("配送员编号为空");
		}
		if(waybillList==null||waybillList.size()==0){
			throw new BaseSupportException("未选择任何运单");
		}
		
		try{
			waybillDao.batchAllocateWaybill2Courier(cid,null, "0", waybillList);
			userDao.updateCourierPsStatus(cid, "1");
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
	}

}
