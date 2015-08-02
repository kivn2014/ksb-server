package com.ksb.openapi.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.ksb.openapi.entity.BuyerEntity;

public class JsonUtils {

	
	public static void main(String[] args) {
		
		String deliver_list = null;
		try {
			deliver_list = "[{name: \""+URLEncoder.encode("测试", "utf-8")+"\", phone: \""+URLEncoder.encode("13483048990", "utf-8")+"\", x: 119.221, y: 39.023, address: \""+URLEncoder.encode("北京市海淀区中关村图书大厦", "utf-8") + "\"}]";
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		handleBuyersInfo(deliver_list);
	}
	
	
	public static void test(){
		
		String deliver_list = null;
		try {
			deliver_list = "[{name: \""+URLEncoder.encode("测试", "utf-8")+"\", phone: \""+URLEncoder.encode("13483048990", "utf-8")+"\", x: 119.221, y: 39.023, address: \""+URLEncoder.encode("北京市海淀区中关村图书大厦", "utf-8") + "\"}]";
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(deliver_list);
		
		JSONArray jsonArray = JSON.parseArray(deliver_list);
		
		for(int i=0;i<jsonArray.size();i++){
			
			String nameEncode = jsonArray.getJSONObject(i).getString("name");
			try {
				System.out.println(URLDecoder.decode(nameEncode, "utf-8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	
	public static List<Map<String, String>> handleBuyersInfo(String buyersInfo){
		
		List<Map<String, String>> list = new ArrayList<Map<String,String>>();
		
		try{
			List<BuyerEntity> parseList = JSON.parseArray(buyersInfo, BuyerEntity.class);
			
			for(BuyerEntity entity : parseList){
				System.out.println(entity.getName());
			}
			
		}catch(Exception e){
			
		}
		
		
		return list;
	}
	
	
	
}
