package com.ksb.openapi.dao;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import com.ksb.openapi.entity.EnterpriseCityEntity;
import com.ksb.openapi.entity.EnterpriseEntity;
import com.ksb.openapi.entity.ShipperEntity;
import com.ksb.openapi.entity.ShipperUserEntity;

import autonavi.online.framework.sharding.dao.constant.ReservedWord;
import autonavi.online.framework.sharding.entry.aspect.annotation.Author;
import autonavi.online.framework.sharding.entry.aspect.annotation.Insert;
import autonavi.online.framework.sharding.entry.aspect.annotation.Select;
import autonavi.online.framework.sharding.entry.aspect.annotation.SingleDataSource;
import autonavi.online.framework.sharding.entry.aspect.annotation.SqlParameter;
import autonavi.online.framework.sharding.entry.aspect.annotation.Update;
import autonavi.online.framework.sharding.entry.entity.CollectionType;

/**
 * 
 * @author houshipeng
 *
 */

@Repository
public class EnterpriseDao {

	/**
	 * 创建企业(发行公司、配送站)
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createShipper(@SqlParameter("m") Map<String, String> map){
		
		StringBuilder sb = new StringBuilder();
		sb.append("insert into shippers(id,name,address,phone,tel,address_x,address_y) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{m.name},#{m.address},#{m.phone},#{m.tel},#{m.x},#{m.y}");
		
		sb.append(")");
		
		return sb.toString();
	}
	
	/**
	 * 商家注册
	 * @param map
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createShipper(@SqlParameter("u") ShipperUserEntity sue,@SqlParameter("eid")String eid){
		
		StringBuilder sb = new StringBuilder();
		sb.append("insert into shippers(id,name,address,phone,address_x,address_y,city,province,enterprise_id) values(");
		
		sb.append("#{u.sp_id},#{u.sp_name},#{u.address},#{u.phone},#{u.address_x},#{u.address_y},#{u.city_name},#{u.province_name},#{eid}");
		
		sb.append(")");
		
		return sb.toString();
	}
	
	
	
	/**
	 * 根据商家名称和电话查询商家信息
	 * @param name
	 * @param phone
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.column,resultType=Long.class)
	public Object queryShipperByNamePhone(@SqlParameter("name") String name,@SqlParameter("phone") String phone){
		
		StringBuilder sb = new StringBuilder("select id from shippers where name=#{name} and phone=#{phone}");
		return sb.toString();
	}
	
	/**
	 * 商家信息检索
	 * @param shipperId
	 * @param shipperName
	 * @param shipperType
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.beanList,resultType=ShipperEntity.class)	
	public Object queryShipperList(@SqlParameter("entity") ShipperEntity shipperEntity){
		
		StringBuilder sb = new StringBuilder("select id,name,phone,address,address_x,address_y,tel from shippers where 1=1 ");
		
		if(StringUtils.isNotBlank(shipperEntity.getId())){
			sb.append(" and id=#{entity.id} ");
		}
		
		if(StringUtils.isNotBlank(shipperEntity.getName())){
			sb.append(" and name like #{entity.name} ");
		}
		if(StringUtils.isNotBlank(shipperEntity.getShipper_type())){
			sb.append(" and shipper_type=#{entity.shipper_type} ");
		}
		return sb.toString();
	}
	
	/**
	 * 查询商家常用地址列表
	 * @param shipperId
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select
	public Object queryShipperAddress(@SqlParameter("sp_id") String shipperId,@SqlParameter("city_name") String cityName){
		
		StringBuilder sb = new StringBuilder("select id,shippers_id,address,address_x,address_y,city_name from shippers_address where shippers_id=#{sp_id} ");
		
		if(StringUtils.isNotBlank(cityName)){
			sb.append(" and city_name=#{city_name} ");
		}
		sb.append(" order by use_times desc limit 10 ");
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateShipperAddress(@SqlParameter("sp_id") String spId,@SqlParameter("sp_name")String spName,@SqlParameter("province") String province,@SqlParameter("city")String cityName,@SqlParameter("address")String address,@SqlParameter("address_detail")String addressDetail,@SqlParameter("city_code") String cityCode,@SqlParameter("x") String x,@SqlParameter("y")String y){
		
		StringBuilder sb = new StringBuilder("update shippers set ");
		sb.append(" address=#{address},address_detail=#{address_detail},address_x=#{x},address_y=#{y},province=#{province},city=#{city},city_code=#{city_code} ");
		if(StringUtils.isNotBlank(spName)){
			sb.append(" ,name=#{sp_name} ");
		}
		
		sb.append(" where id=#{sp_id} ");
		
		return sb.toString();
	}
	

	/**
	 * 
	 * @param cityName 城市名称(后面带 市)
	 * @param adcode 六位的区号码(例如北京海淀 100080)
	 * @param cityCode 城市编号(例如北京 010)
	 * @param district 区(例如北京 海淀区)
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.beanList,resultType=EnterpriseCityEntity.class)
	public Object queryEnterpriseByCityInfo(@SqlParameter("city_name") String cityName,@SqlParameter("adcode")String adcode,@SqlParameter("city_code")String cityCode,@SqlParameter("district")String district){
		
		StringBuilder sb = new StringBuilder("select enterprise_id,city_name,province_name,status,adcode,district,ifnull(manual,'0') manual from enterprise_city where status=0 ");
		
		if(StringUtils.isNotBlank(cityCode)){
			sb.append(" and city_code=#{city_code}");
		}
		if(StringUtils.isNotBlank(cityName)){
			sb.append(" and city_name=#{city_name} ");
		}
		
		if(StringUtils.isNotBlank(adcode)){
			sb.append(" and adcode=#{adcode} ");
		}
		
		if(StringUtils.isNotBlank(district)){
			sb.append(" and district=#{district} ");
		}
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createEnterprise(@SqlParameter("e") EnterpriseEntity enterpriseEntity){
		
		StringBuilder sb = new StringBuilder("insert into enterprise(id,name,province,city,district,status,contact,tel,address,description) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{e.name},#{e.province},#{e.city},#{e.district},#{e.status},#{e.contact},#{e.tel},#{e.address},#{e.description} ");
		
		sb.append(")");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createShipper(@SqlParameter("e") ShipperEntity spEntity,@SqlParameter("eid") String eid){
		
		StringBuilder sb = new StringBuilder("insert into shippers(id,name,province,city,district,status,contact,tel,address,enterprise_id) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{e.name},#{e.province},#{e.city},#{e.district},#{e.status},#{e.contact},#{e.tel},#{e.address},#{eid} ");
		
		sb.append(")");
		
		return sb.toString();
	}	
}
