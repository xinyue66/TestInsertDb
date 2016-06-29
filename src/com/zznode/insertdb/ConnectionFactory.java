package com.zznode.insertdb;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionFactory {
	
	private static String driver=null;
	private static String url=null;
	private static String user=null;
	private static String password=null;
	
	
	/**
	 * 用于判断数据库类型;true为informix,false为oracle
	 */
	public static boolean dbfal = true;  //用于判断数据库类型;true为informix,false为oracle
	
	public ConnectionFactory(int num) {
		Properties pro=new Properties();
		File file = new File("conf/server.properties");
		InputStream in = null;
		try {
			in = new FileInputStream(file);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		try {
			pro.load(in);
			driver=pro.getProperty("db"+num+".driver");
			url=pro.getProperty("db"+num+".url");
			user=pro.getProperty("db"+num+".user");
			password=pro.getProperty("db"+num+".password");
			
		}catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public Connection getConnection(){
		
		Connection conn=null;
		try{
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		return conn;
	}
	
	
	/**
	 * 设置事物的隔离级别
	 * @param num 1:允许脏读,2.不允许脏读,4.能读到提前前的数据,8.防止脏读、单读和虚读
	 * @return
	 */
	public Connection getConnection(int num){
		
		Connection conn=null;
		try{
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);

			if(dbfal){
				if(num==1 || num==2 || num == 4 || num==8 ){
					conn.setTransactionIsolation(num);
				}
			}
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return conn;
	}
	
	
	public void close(ResultSet rs,Statement stmt){
		try {
			if(rs!=null)
				rs.close();
			if(stmt!=null)
				stmt.close();
		} catch (Exception e) {}
	}
	
	public void close(Connection conn){
		try {
			if(conn!=null)
				conn.close();
		} catch (Exception e) {}
	}
}
