package com.ht.util;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/** 
 * 连接mongodb类
* @author Qiu 
* @datetime 2018年3月4日 上午11:33:25 
*/

public class MongodbConnection {
	
	/**
	 * 连接mongodb  (无账号密码认证)
	 * @param ip 需要连接mongodb服务器的ip
	 * @param port 需要连接mongdb服务器的端口
	 * @param databasename 需要连接mongodb服务器的数据库名
	 * @param tablename 需要连接mongodb所选择的数据库的表名
	 * @return 如果有该数据库 返回该数据库  没有该数据库则创建后返回创建的数据库
	 */
	public MongoCollection<Document> getConnection(String ip,Integer port,String databasename,String tablename) {
		
	
		MongoCollection<Document> collection=null;
		try {
		// 连接到 mongodb 服务
        MongoClient mongoClient = new MongoClient( ip , port );
		
       // 连接到数据库
       MongoDatabase mongoDatabase = mongoClient.getDatabase(databasename); 
       
       //选择数据库的集合/表名
       collection = mongoDatabase.getCollection(tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return collection;
	}
	/**
	 * 连接mongodb  (有账号密码认证)
	 * @param ip 需要连接mongodb服务器的ip
	 * @param ip 需要连接mongodb服务器的ip
	 * @param databasename 需要连接mongodb服务器的数据库名
	 * @param username 连接mongodb的用户名
	 * @param pwd  连接mongodb的密码
	 * @param tablename 需要连接mongodb所选择的数据库的表名
	 * @return 如果有该数据库 返回该数据库  没有该数据库则创建后返回创建的数据库
	 */
	public MongoCollection<Document> getConnectionUsePwd(String ip,Integer port,String databasename,String username,String pwd,String tablename) {
		
		MongoCollection<Document> collection=null;
		try {
			//连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址  
            //ServerAddress()两个参数分别为 服务器地址 和 端口  
            ServerAddress serverAddress = new ServerAddress(ip,port);  
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();  
            addrs.add(serverAddress);  
              
            //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码  
            MongoCredential credential = MongoCredential.createScramSha1Credential(username, databasename, pwd.toCharArray());  
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();  
            credentials.add(credential);  
              
            //通过连接认证获取MongoDB连接  
            MongoClient mongoClient = new MongoClient(addrs,credentials);  
              
            //连接到数据库  
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databasename);  
			
            //选择数据库的集合/表名
            collection = mongoDatabase.getCollection(tablename);
            
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return collection;
	}
	
}
