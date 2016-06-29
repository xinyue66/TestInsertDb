package com.zznode.insertdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.log4j.Logger;

public class InsertDataToDb implements Runnable{

	private static final Logger logger1 = Logger.getLogger("Insert");
	private ConnectionFactory factory;
	private Connection conn = null;
	private int insertNum,insertCount;
	
	public InsertDataToDb(ConnectionFactory factory,int insertNum,int insertCount) {
		this.factory = factory;
		this.insertNum = insertNum;
		this.insertCount = insertCount;
	}

	public void insert(int insertNum,int insertCount) throws Exception {
		conn = factory.getConnection();
		long init_time = System.currentTimeMillis();
		long prev_time = System.currentTimeMillis();
		long curr_time = System.currentTimeMillis();
		PreparedStatement resDataSQL = null;
		String batchsql = "INSERT INTO testid (ID,NUM,NUM1,NUM2,NUM3,NUM4) values(?,?,?,?,?,?)";
		try {
			resDataSQL = conn.prepareStatement(batchsql);
			int num = 0;
			for(int j=0; j<insertCount; j++) {
				prev_time = curr_time;
				for (int i=0; i<insertNum; i++) {
					num++;
					resDataSQL.setString(1,uuid());
					resDataSQL.setString(2, "f1"+num);
					resDataSQL.setString(3, "f2"+num);
					resDataSQL.setString(4, "f3"+num);
					resDataSQL.setString(5, "f4"+num);
					resDataSQL.setString(6, "f5"+num);
					resDataSQL.addBatch();						
				}			
				resDataSQL.executeBatch();
	
				curr_time = System.currentTimeMillis();
				if (curr_time-init_time>1000)
					logger1.info("one time use time = " + (curr_time-prev_time) + " ms, total time = " + (curr_time-init_time)/1000 + " s, rate = " + num/((curr_time-init_time)/1000) );
			}
			resDataSQL.close();
			factory.close(conn);
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		} finally {
			resDataSQL.close();
			factory.close(conn);
		}
	}

    public String uuid() {
		return UUID.randomUUID().toString().replace("-", "").toUpperCase();
	}

	@Override
	public void run() {
		try {
			insert(insertNum,insertCount);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
