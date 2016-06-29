package com.zznode.insertdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;


public class QueryDataCount implements Callable<Integer>{
	
	private Connection conn = null;
	private Statement st = null;
	private ConnectionFactory factory;
	
	public QueryDataCount(ConnectionFactory factory) {
		this.factory = factory;
	}

	@Override
	public Integer call() throws Exception {
		int result = 0;
		ResultSet rs = null;
		String sql = "select count(id) from testid";
		try {
			conn = factory.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(sql);	
			while (rs.next()) {
				result = rs.getInt(1);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			factory.close(conn);
		}
		return new Integer(result);
	}
}
