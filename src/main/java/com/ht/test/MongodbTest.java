package com.ht.test;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.ht.util.MongodbConnection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/** 
* @author Qiu 
* @datetime 2018年3月4日 上午11:59:51 
*/

public class MongodbTest {
	private MongodbConnection connection=new MongodbConnection();

	/**
	 * 
	 * 向文档/表 中插入数据
	 * @param ip 连接mongdb服务器的ip
	 * @param port 连接mongdb服务器的端口号
	 * @param databasename 连接mongodb的数据库名称
	 * @param tablename  连接数据库里的表名
	 */
	public void insertCollection(String ip, Integer port, String databasename,String tablename) {
		
		//获取数据库表
		MongoCollection<Document> collection=connection.getConnection(ip, port,databasename, tablename);
		
		 /** 
		  * 相当于创建表每一行的数据
         * 1. 创建文档 org.bson.Document 参数为key-value的格式 
         * 2. 创建文档集合List<Document> 
         * 3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用 mongoCollection.insertOne(Document) 
		 */
		Document document=new Document("name","小明").append("age", 18).append("sex", "男");
		Document document1=new Document("title","mongodb").append("by", "菜鸟教程").append("url", "www.runbbo.com");

		//把要插入集合/表的数据  存在list里面
		List<Document> datalist=new ArrayList<Document>();
		datalist.add(document);
		datalist.add(document1);
		
		//把数据插入集合/表
		collection.insertMany(datalist);
		
		System.out.println("插入成功");
	}
	
	/**
	 * 
	 * 查询文档/表 中的所有数据
	 * @param ip 连接mongdb服务器的ip
	 * @param port 连接mongdb服务器的端口号
	 * @param databasename 连接mongodb的数据库名称
	 * @param tablename  连接数据库里的表名
	 */
	public void findCollection(String ip, Integer port, String databasename,String tablename) {
		//获取数据库表
		MongoCollection<Document> collection=connection.getConnection(ip, port,databasename, tablename);
		
        /**
         * 检索所有的文档/列  
        * 1. 获取迭代器FindIterable<Document> 
        * 2. 获取游标MongoCursor<Document> 
        * 3. 通过游标遍历检索出的文档集合 
        * */  
        FindIterable<Document> findIterable = collection.find();  
        MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){  
           System.out.println(mongoCursor.next());  
        }  
	}
	
	/**
	 * 
	 * 更新文档/表 中的数据
	 * @param ip 连接mongdb服务器的ip
	 * @param port 连接mongdb服务器的端口号
	 * @param databasename 连接mongodb的数据库名称
	 * @param tablename  连接数据库里的表名
	 */
	public void updateCollection(String ip, Integer port, String databasename,String tablename) {
		//获取数据库表
		MongoCollection<Document> collection=connection.getConnection(ip, port,databasename, tablename);
		
		//更新文档   将文档中age=18 改成age=20
		//$set的意思是只改age=20   该列的其他数据不会被修改  如果不加$set的话 这段话的意思就变成了 把age=18的数据(1条) 改成age=20 其他数据清空
        collection.updateMany(Filters.eq("age", 18), new Document("$set",new Document("age",20)));
		
		//查看结果
        FindIterable<Document> findIterable = collection.find();  
        MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){  
           System.out.println(mongoCursor.next());  
        }  
	}
	
	/**
	 * 
	 * 删除文档/表 中的数据
	 * @param ip 连接mongdb服务器的ip
	 * @param port 连接mongdb服务器的端口号
	 * @param databasename 连接mongodb的数据库名称
	 * @param tablename  连接数据库里的表名
	 */
	public void deleteCollection(String ip, Integer port, String databasename,String tablename) {
		//获取数据库表
		MongoCollection<Document> collection=connection.getConnection(ip, port,databasename, tablename);
		
		 //删除符合条件的第一个文档  
         //collection.deleteOne(Filters.eq("x", 1));  
         //删除所有符合条件的文档  
         //collection.deleteMany (Filters.eq("x", 1));  
        
		
        //检索查看结果  
        FindIterable<Document> findIterable = collection.find();  
        MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){  
          System.out.println(mongoCursor.next());  
        } 
	}
	
	
	public static void main(String[] args) {
		MongodbTest test=new MongodbTest();
		//向集合/表 中插入数据测试
		//test.insertCollection("192.168.3.15",27017,"runoob","tab1");
		
		//检索集合/表中的 文档/列中的所有数据
		//test.findCollection("192.168.3.15",27017,"runoob","tab1");
		
		//更新集合/表 中的数据
		//test.updateCollection("192.168.3.15",27017,"runoob","tab1");
		
		//删除集合/表 中的数据
		test.deleteCollection("192.168.3.15",27017,"runoob","tab1");
		
	}
}
