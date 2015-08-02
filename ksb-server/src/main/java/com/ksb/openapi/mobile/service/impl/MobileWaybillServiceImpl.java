package com.ksb.openapi.mobile.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import autonavi.online.framework.sharding.dao.DaoHelper;

import com.ksb.openapi.dao.WaybillDao;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.mobile.service.MobileWaybillService;

@Service
public class MobileWaybillServiceImpl implements MobileWaybillService {

	@Autowired
	WaybillDao waybillDao;
	

	@Override
	public Map<String, Object> searchWaybillByCourier(String courierId,
			String waybillId, String waybillStatus, int skip, int size) {
		// TODO Auto-generated method stub
		
		/*必须得参数快递员ID不能为空*/
		if(StringUtils.isBlank(courierId)){
			throw new BaseSupportException("必须得参数：配送员编号[courierId] 为空");
		}
		
		List<Map<String, Object>> rsList = null;
		try{
			rsList = (List<Map<String, Object>>) waybillDao.searchWaybillInfoByCourier(courierId, waybillId, waybillStatus, skip, size);
		}catch(Exception e){
			throw new BaseSupportException(e);
		}
		
		Map<String, Object> rsMap = new HashMap<String, Object>();
		rsMap.put("1", converRsListID(rsList));
		rsMap.put("2", DaoHelper.getCount());
		
		return rsMap;
	}

	private List<Map<String, Object>> converRsListID(List<Map<String, Object>> list){
		
		if(list==null||list.size()==0){
			return list;
		}
		List<Map<String, Object>> rsList = new ArrayList<Map<String,Object>>();
		
		for(Map<String, Object> m : list){
			Object objId = m.get("id");
			m.put("id", objId.toString());
			rsList.add(m);
		}
		return rsList;
	}
	
	

	@Override
	public boolean updateWaybillStatusByCourier(String waybillId,
			String courierId, String waybillStatus) {
		// TODO Auto-generated method stub

		try {
			waybillDao.updateWaybillStatusByKSBId(Long.parseLong(waybillId),waybillStatus);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	
}
