package com.ksb.openapi.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import com.ksb.openapi.entity.BuyerEntity;
import com.ksb.openapi.entity.CourierEntity;
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
 * 用户管理(各类用户：买家、配送员、系统登录用户等)
 * @author houshipeng
 *
 */

@Repository
public class UserDao {

	/**
	 * 记录第三方平台的配送员信息
	 *     第三方平台的配送员 默认密码是 123456
	 *     name和real_name相同
	 *     user_type为 TP（third part）
	 * @param map
	 * @deprecated
	 * 参考 createCourier 方法
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createThridPSUser(@SqlParameter("m") Map<String, String> map){
		
		/*新创建的用户状态为0 表示开通可用*/
		String status = "0";
		/*第三方平台的快递员类型为固定值  TP*/
		String user_type = "TP";
		/*第三方平台的快递员统一挂到 企业id为0下面(快递员虚拟企业)*/
		String enterprise_id = "0";
		/*o2o 配送员地址是可选的(如果有记录的是家庭地址)*/
		String address = "no";
		if(map.get("address")==null){
			map.put("address", address);
		}
		String password = "123456";
		
		map.put("password", password);
		map.put("status", status);
		map.put("user_type", user_type);
		map.put("enterprise_id", enterprise_id);
		StringBuilder sb = new StringBuilder();
		sb.append("insert into courier(id,name,password,real_name,status,phone,address,user_type,enterprise_id) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{m.name},#{m.password},#{m.name},#{m.status},#{m.phone},#{m.address},#{m.user_type},#{m.enterprise_id}");
		sb.append(")");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createBuyer(@SqlParameter("m") Map<String, String> map){
		
		/*新创建的用户状态为0 表示开通可用*/
		String status = "0";
		/*买家用户类型为固定值  CU*/
		String user_type = "CU";
		/*真实的买家统一挂到 企业id为1下面(买家虚拟企业)*/
		String enterprise_id = "1";
		String password = "123456";
		
		/*基本的参数校验,买家的 联系电话，地址、姓名不允许为空*/
		
		map.put("password", password);
		map.put("status", status);
		map.put("user_type", user_type);
		map.put("enterprise_id", enterprise_id);
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into user(id,name,password,real_name,status,phone,address,user_type,enterprise_id,address_x,address_y) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{m.name},#{m.password},#{m.name},#{m.status},#{m.phone},#{m.address},#{m.user_type},#{m.enterprise_id},#{m.x},#{m.y}");
		sb.append(")");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createBuyer(@SqlParameter("be") BuyerEntity buyerEntity){
		
		/*新创建的用户状态为0 表示开通可用*/
		String status = "0";
		/*买家用户类型为固定值  CU*/
		String user_type = "CU";
		/*真实的买家统一挂到 企业id为1下面(买家虚拟企业)*/
		String enterprise_id = "1";
		String password = "123456";
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into user(id,name,password,real_name,status,phone,address,user_type,enterprise_id,address_x,address_y) values(");
		
		sb.append("#{"+ReservedWord.snowflake+"},#{be.name},'"+password+"',#{be.name},'"+status+"',#{be.phone},#{be.address},'"+user_type+"',"+enterprise_id+",#{be.address_x},#{be.address_y}");
		sb.append(")");
		
		return sb.toString();
	}
	
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.column,resultType=Long.class)
	public Object queryBuyerIDByNamePhone(@SqlParameter("name") String userName,@SqlParameter("phone") String userPhone){
		
		/*所有买家*/
		StringBuilder sb = new StringBuilder();
		sb.append("select id from user where name=#{name} and phone=#{phone} and enterprise_id=1");
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean, resultType=BuyerEntity.class)
	public Object queryBuyerInfoByNamePhone(@SqlParameter("name") String userName,@SqlParameter("phone") String userPhone){
		
		/*所有买家*/
		StringBuilder sb = new StringBuilder();
		sb.append("select id,name,address,address_x,address_y,phone,status,user_type from user where name=#{name} and phone=#{phone} and enterprise_id=1");
		return sb.toString();
	}
	
	/**
	 * 快递员登录客户端APP验证
	 * @param userName
	 * @param passwd
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean,resultType=CourierEntity.class)
	public Object authenCourier(@SqlParameter("un") String userName,@SqlParameter("pw") String passwd){
		
		StringBuilder sb = new StringBuilder();
		sb.append(" select c.id,c.name,c.real_name,c.status,c.work_status,c.phone,c.star_level,e.name enterprise_name,e.id enterprise_id from courier c join enterprise e on c.enterprise_id = e.id ");
		sb.append(" where 1=1 ");
		sb.append(" and (c.name=#{un} or c.phone=#{un}) and c.password=#{pw} ");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean,resultType=ShipperUserEntity.class)	
	public Object authenShipperUser(@SqlParameter("un") String userName,@SqlParameter("pw") String passwd){
		
		StringBuilder sb = new StringBuilder("select spu.id uid,sp.id sp_id,case when sp.address is null then '0' else '1' end new_user,spu.name,spu.nick_name,spu.real_name,spu.status,spu.phone,");
		sb.append(" sp.address,sp.address_detail,sp.address_x,sp.address_y,sp.city city_name,sp.city_code city_code,sp.name sp_name ");
		sb.append(" from shipper_user spu left join shippers sp on spu.shippers_id = sp.id  where 1=1 ");
		
		sb.append(" and (spu.name=#{un} or spu.phone=#{un}) and spu.passwd=#{pw} ");
		return sb.toString();
	}
	
	
	
	/**
	 * 快递员修改登录客户端APP密码
	 * @param userName
	 * @param oldPwd
	 * @param newPwd
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateCourierPasswd(@SqlParameter("eid") String eid,@SqlParameter("un") String userName,@SqlParameter("opw") String oldPwd,@SqlParameter("npw") String newPwd,@SqlParameter("srcpwd") String srcPassword){
		
		StringBuilder sb = new StringBuilder("update courier set password=#{npw},src_password=#{srcpwd} where name=#{un} and password=#{opw}");
		return sb.toString();
	}

	/**
	 * 商家版APP修改登录密码
	 * @param userName
	 * @param oldPwd
	 * @param newPwd
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateShipperUserPasswd(@SqlParameter("un") String userName,@SqlParameter("opw") String oldPwd,@SqlParameter("npw") String newPwd,@SqlParameter("srcpwd") String srcPassword){
		
		StringBuilder sb = new StringBuilder("update shipper_user set passwd=#{npw},src_passwd=#{srcpwd} where (name=#{un} or phone=#{un}) and passwd=#{opw}");
		return sb.toString();
	}

	/**
	 * 更新配送员工作状态
	 * @param cid
	 * @param workStatus
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateCourierWorkStatus(@SqlParameter("cid") String cid,@SqlParameter("work_status") String workStatus,@SqlParameter("x") String x,@SqlParameter("y") String y){
	
		StringBuilder sb = new StringBuilder("update courier set work_status=#{work_status} ");
		
		if(StringUtils.isNotBlank(x) && StringUtils.isNotBlank(y)){
			sb.append(" ,address_x=#{x},address_y=#{y} ");
		}
		
		sb.append(" where id=#{cid} ");
		return sb.toString();
	}
	
	/**
	 * 更新配送员配送状态
	 * @param cid
	 * @param workStatus
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateCourierDeliveryStatus(@SqlParameter("cid") String cid,@SqlParameter("status") String status){
		StringBuilder sb = new StringBuilder("update courier set delivery_status=#{status} where id=#{cid} ");
	
		return sb.toString();
	}
	
	/**
	 * 更新配送员位置
	 * @param cid
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateCourierGps(@SqlParameter("cid") String cid,@SqlParameter("x") String x,@SqlParameter("y") String y){
	
		StringBuilder sb = new StringBuilder("update courier set address_x=#{x},address_y=#{y} where id=#{cid} ");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Update
	public Object updateCourierPsStatus(@SqlParameter("cid") String cid, @SqlParameter("ps_status")String deliveryStatus){
		
		StringBuilder sb = new StringBuilder("update courier set delivery_status=#{ps_status} where id=#{cid}");
	
		return sb.toString();
	}
	
	/**
	 * 根据条件查询快递员信息(根据ID或者手机号)
	 * @param uid
	 * @param name
	 * @param phone
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.bean,resultType=CourierEntity.class)
	public Object queryCourierInfo(@SqlParameter("id") String uid,@SqlParameter("real_name") String realname,@SqlParameter("phone") String phone){
		
		StringBuilder sb = new StringBuilder(" select c.id,c.name,c.real_name,c.status,c.work_status,c.phone,c.star_level,e.name enterprise_name,e.id enterprise_id from courier c join enterprise e on c.enterprise_id = e.id ");
		sb.append(" where 1=1 and status=0 and work_status=1 ");
		
		/*拼装查询条件*/
		if(StringUtils.isNotBlank(uid)){
			sb.append(" and c.id=#{id} ");
		}
		if(StringUtils.isNotBlank(realname)){
			sb.append(" and real_name like #{real_name} ");
		}
		if(StringUtils.isNotBlank(phone)){
			sb.append(" and c.phone=#{phone} ");
		}
		return sb.toString();
	}

	

	
	/**
	 * 新建一个快递员
	 * @param courierEntity
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert		
	public Object createCourier(@SqlParameter("ce") CourierEntity courierEntity){
		
		StringBuilder sb = new StringBuilder(" insert into courier(id,name,real_name,status,work_status,phone,star_level,enterprise_id,password,src_password,delivery_status) values ");
		sb.append("(#{"+ReservedWord.snowflake+"},#{ce.name},#{ce.real_name},0,#{ce.work_status},#{ce.phone},0,#{ce.enterprise_id},#{ce.pwd},#{ce.srcPwd},#{ce.delivery_status})");
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert		
	public Object batchCreateCourier(@SqlParameter("list") List<CourierEntity> list){
		
		StringBuilder sb = new StringBuilder(" insert into courier(id,name,real_name,status,work_status,phone,star_level,enterprise_id,password,src_password,delivery_status) values ");
		sb.append("(#{"+ReservedWord.snowflake+"},#{list."+ReservedWord.index+".name},#{list."+ReservedWord.index+".real_name},0,#{list."+ReservedWord.index+".work_status},#{list."+ReservedWord.index+".phone},0,#{list."+ReservedWord.index+".enterprise_id},#{list."+ReservedWord.index+".pwd},#{list."+ReservedWord.index+".srcPwd},#{list."+ReservedWord.index+".delivery_status})");
		return sb.toString();
	}
	
	/**
	 * 
	 * @param map
	 * @deprecated
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert
	public Object createCourier(@SqlParameter("m") Map<String, Object> map){
		
		/*快递员类型，报社下得快递员(newspaper courier)   放到service 层去封装*/
//		String userType = "NC";
//		map.put("user_type", userType);
		
		StringBuilder sb = new StringBuilder("insert into courier(");
		sb.append("id,name,real_name,password,status,phone,user_type,address,enterprise_id");
		sb.append(") ");
		
		sb.append("values(");
		sb.append("#{"+ReservedWord.snowflake+"},#{m.name},#{m.real_name},#{m.password},'0',#{m.phone},#{m.user_type},#{m.address},#{m.enterprise_id}");
		sb.append(")");
		
		return sb.toString();
	}
	
	/**
	 * 配送员检索
	 * @param entity
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.beanList,resultType= CourierEntity.class,paging = @Paging(skip = "skip", size = "size"), queryCount = true)
	public Object queryCourierInList(@SqlParameter("entity") CourierEntity entity,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		StringBuilder sb = new StringBuilder("select c.id,c.name,c.real_name,c.phone,c.star_level,c.status,c.work_status,c.delivery_status,e.id enterprise_id,c.address_x,c.address_y,e.name enterprise_name ");
		
		sb.append(" from courier c join enterprise e on c.enterprise_id=e.id ");
		sb.append(" where 1=1");
		
		/*cid*/
		if(StringUtils.isNotBlank(entity.getId())){
			sb.append(" and c.id=#{entity.id} ");
		}
		
		/*用户名(like 匹配)*/
		if(StringUtils.isNotBlank(entity.getName())){
			entity.setName("%"+entity.getName()+"%");
			sb.append(" and c.name like #{entity.name} ");
		}
		/*用户真实姓名(like匹配)*/
		if(StringUtils.isNotBlank(entity.getReal_name())){
			entity.setReal_name("%"+entity.getReal_name()+"%");
			sb.append(" and c.real_name like #{entity.real_name} ");
		}
		/*手机号*/
		if(StringUtils.isNotBlank(entity.getPhone())){
			entity.setPhone("%"+entity.getPhone()+"%");
			sb.append(" and c.phone like #{entity.phone} ");
		}
		
		/*隶属企业*/
		if(StringUtils.isNotBlank(entity.getEnterprise_id())){
			sb.append(" and e.id=#{entity.enterprise_id} ");
		}
		/*用户状态*/
		if(StringUtils.isNotBlank(entity.getStatus())){
			sb.append(" and c.status=#{entity.status} ");
		}
		/*工作状态*/
		if(StringUtils.isNotBlank(entity.getWork_status())){
			sb.append(" and c.work_status=#{entity.work_status} ");
		}
		/*工作状态*/
		if(StringUtils.isNotBlank(entity.getDelivery_status())){
			sb.append(" and c.delivery_status=#{entity.delivery_status} ");
		}	
		sb.append(" order by id desc ");
		
		return sb.toString();
	}
	
	/**
	 * 商家登录用户注册
	 * @param spUser
	 * @return
	 */
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Insert	
	public Object createShipperUser(@SqlParameter("u") ShipperUserEntity spUser){
		
		StringBuilder sb = new StringBuilder();
		sb.append("insert into shipper_user ");
		
		sb.append("(id,shippers_id,name,nick_name,real_name,passwd,src_passwd,status,phone)");
		
		sb.append(" values(#{u.uid},#{u.sp_id},#{u.name},#{u.nick_name},#{u.real_name},#{u.passwd},#{u.src_passwd},0,#{u.phone})");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select		
	public Object queryNearCourier(@SqlParameter("eid") String eid,@SqlParameter("x")String x,@SqlParameter("y")String y,@SqlParameter("dis")int distance){
		
		StringBuilder sb = new StringBuilder();
		sb.append("select id,address_x,address_y,enterprise_id,");
		sb.append(" ROUND(6378.138*2*ASIN(SQRT(POW(SIN((#{y}*PI()/180-address_y*PI()/180)/2),2)+COS(#{y}*PI()/180)*COS(address_y*PI()/180)*POW(SIN((#{x}*PI()/180-address_x*PI()/180)/2),2)))*1000) as distance ");
		sb.append("FROM courier where enterprise_id in (#{eid}) and work_status=1 and delivery_status=0 ");
		
		sb.append(" having distance<=#{dis} ");
		
		sb.append(" ORDER BY distance limit 1");
		
		return sb.toString();
	}
	
	@Author("shipeng.hou")
	@SingleDataSource(1)
	@Select(collectionType=CollectionType.beanList,resultType=ShipperUserEntity.class,paging = @Paging(skip = "skip", size = "size"), queryCount = true)	
	public Object querySpUser(@SqlParameter("eid")String eid,@SqlParameter("sp_name")String spName,@SqlParameter("skip") int skip,@SqlParameter("size") int size){
		
		StringBuilder sb = new StringBuilder("select spu.id,sp.address address,sp.address_detail address_detail,sp.address_x address_x,sp.address_y address_y,sp.city city_name,spu.name,spu.phone,sp.name sp_name,spu.status from shipper_user spu join shippers sp on spu.shippers_id=sp.id where 1=1 ");
		sb.append(" and sp.enterprise_id=#{eid} ");
		if(StringUtils.isNotBlank(spName)){
			sb.append(" and sp.name like #{sp_name} ");
		}
		sb.append(" order by spu.id desc");
		return sb.toString();
	}
	
}
