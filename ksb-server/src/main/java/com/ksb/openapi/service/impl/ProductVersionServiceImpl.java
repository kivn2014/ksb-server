package com.ksb.openapi.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ksb.openapi.dao.ProductVersionDao;
import com.ksb.openapi.entity.ProductVersionEntity;
import com.ksb.openapi.error.BaseSupportException;
import com.ksb.openapi.service.ProductVersionService;

@Service
public class ProductVersionServiceImpl implements ProductVersionService {

	@Autowired
	ProductVersionDao productVersionDao;
	
	@Override
	public ProductVersionEntity queryLatestVersion(String productType) {
		// TODO Auto-generated method stub
		if(StringUtils.isBlank(productType)){
			throw new BaseSupportException("产品类型为空");
		}
		
		ProductVersionEntity entity = null;
		try{
			entity = (ProductVersionEntity)productVersionDao.queryAppLatestVersion(productType);
		}catch(Exception e){
			throw new BaseSupportException(e.getMessage());
		}
		
		return entity;
	}

	@Override
	public void addAppVersion(ProductVersionEntity productVersionEntity) {
		// TODO Auto-generated method stub
		
		if(productVersionEntity==null){
			throw new BaseSupportException("产品版本对象对空");
		}
		
		if(StringUtils.isBlank(productVersionEntity.getProduct_type())){
			throw new BaseSupportException("未指定产品类型");
		}
		if(StringUtils.isBlank(productVersionEntity.getVersion_code())){
			throw new BaseSupportException("未指定产品版本号");
		}
		if(productVersionEntity.getVersion_num()==null){
			throw new BaseSupportException("未指定产品版本比较基数");
		}
		
		try{
			productVersionDao.createProductVersion(productVersionEntity);
		}catch(Exception e){
			throw new BaseSupportException(e.getMessage());
		}

	}

}
