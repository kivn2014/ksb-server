package com.ksb.openapi.dao;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.CourierEntity;
import com.ksb.openapi.entity.ProductVersionEntity;
import com.ksb.openapi.entity.ShipperUserEntity;
import com.ksb.openapi.util.MD5Util;

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
 * APP版本管理
 * @author houshipeng
 *
 */

@Repository
public class ProductVersionDao {

	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createProductVersion(@SqlParameter("p") ProductVersionEntity entity){
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into product_version(id,version_num,version_code,product_type,publish_time) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{p.version_num},#{p.version_code},#{p.product_type},(UNIX_TIMESTAMP() * 1000)");
		sb.append(")");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean,resultType=ProductVersionEntity.class)
	public Object queryAppLatestVersion(@SqlParameter("pt") String productType){
		
		/*所有买家*/
		StringBuilder sb = new StringBuilder();
		sb.append("select id,version_num,version_code,product_type,FROM_UNIXTIME((publish_time/1000),'%Y-%m-%d %T') publish_time from product_version where product_type=#{pt} ");
		sb.append(" order by publish_time desc limit 1 ");
		return sb.toString();
	}
}
