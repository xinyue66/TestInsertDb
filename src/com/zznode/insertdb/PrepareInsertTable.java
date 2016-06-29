package com.zznode.insertdb;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class PrepareInsertTable {
	
	private static final Logger logger1 = Logger.getLogger("Console");
	private ConnectionFactory factory;
	
	public PrepareInsertTable(ConnectionFactory factory) {
		this.factory = factory;
	}

	public void truncateTable(){
		Connection conn = null;
		Statement st = null;
		try {
			conn = factory.getConnection();
			st = conn.createStatement();
			String sql = "truncate table testid";
			st.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			factory.close(conn);
		}
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger1.info("预先清空testid表任务完成。");
	}
	
	public void create(){
		Connection conn = null;
		Statement st = null;
		try {
			conn = factory.getConnection();
			st = conn.createStatement();
			String sql = "CREATE TABLE testid(	id varchar(32) NOT NULL,"
					+ "num varchar(32),num1 varchar(32),num2 varchar(32),"
					+ "num3 varchar(32),num4 varchar(32),PRIMARY KEY (id));";
			st.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			factory.close(conn);
		}
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger1.info("预先创建testid表任务完成。");
	}
}
