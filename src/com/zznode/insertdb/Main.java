package com.zznode.insertdb;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


public class Main {
	private static final Logger logger1 = Logger.getLogger("Main");
	private static final Logger logger2 = Logger.getLogger("Query");
	
	private static int totality = 500000;
	private static int theadCount = 10;//插入线程数；
	private static int insertNum = 1000;//一次插入数据量
	//是否新建测试表,是否要清空数据
	private static int prepareFlag = 0; //create,truncate,nothing
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
	private static Timer timer;
	private static TimerTask timerTask;
	private static int masterCount;
	private static int slaveCount;
	//创建连接工厂类
	private static ConnectionFactory factoryMaster = new ConnectionFactory(1);
	private static ConnectionFactory factorySlave = new ConnectionFactory(2);
	private static int insertCount = totality/(theadCount * insertNum); //计划执行insert的次数；
	//创建查询类
	private final static QueryDataCount queryMaster = new QueryDataCount(factoryMaster);
	private final static QueryDataCount querySlave = new QueryDataCount(factorySlave);
	
	public static void main(String[] args) {
		if (args.length!=0) {
			if (args.length!=4) {
				for (int i = 0; i < args.length; i++) {
					System.out.println(i+"--"+args[i]);
				}
				System.err.println("参数不对，请重新执行！");
				System.err.println("./startInsertDBTest.sh 5000000 20 2000 0");
				System.err.println("./startInsertDBTest.sh 插入总数 线程数 "
						+ "一次插入数据量 是否新建或清空测试表（1：创建；2：清空；0：什么也不做。");

				System.exit(0);
			}
			totality = Integer.valueOf(args[0]);
			theadCount = Integer.valueOf(args[1]);
			insertNum = Integer.valueOf(args[2]);
			prepareFlag = Integer.valueOf(args[3]);
		}
		logger1.info("本次同时插入的线程数为："+theadCount);
		logger1.info("计划执行insert的次数："+insertCount);
		logger1.info("一次一个线程插入的数据量为："+insertNum);
		logger1.info("本次最大插入的数据量为："+insertNum*insertCount*theadCount);

		//是否执行创建测试表和清空测试表数据操作。
		if (prepareFlag==1) {
			PrepareInsertTable prepareInsertTable = new PrepareInsertTable(factoryMaster);
			prepareInsertTable.create();	
		}else if (prepareFlag==2) {
			PrepareInsertTable prepareInsertTable = new PrepareInsertTable(factoryMaster);
			prepareInsertTable.truncateTable();		
		}else {
			logger1.info("是否新建或清空测试表参数不对，默认什么也不做直接插入数据。");
		}
		
		//创建插入数据的线程池执行器
		ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();
		//创建插入数据的线程
		InsertDataToDb insert = new InsertDataToDb(factoryMaster, insertNum, insertCount);
		//开始执行
		Date startTime = new Date();
		logger1.info("插入开始时间：" + df.format(startTime));
		
		for (int i = 0; i < theadCount; i++) {
			executor.execute(insert);
		}
		executor.shutdown();//不再插入线程，等待线程结束。
		//执行定时查询任务；
		execTimerTask();
		try {
		      // 请求关闭、发生超时或者当前线程中断，无论哪一个首先发生之后，都将导致阻塞，直到所有任务完成执行
		      // 设置最长等待3小时
			executor.awaitTermination(3, TimeUnit.HOURS);
		    } catch (InterruptedException e) {
		      e.printStackTrace();
		      logger1.error("插入时间超长，系统结束。"); 
		      System.exit(1);
		    }
		timer.cancel();//插入数据结束后，timer任务结束。
		Date endTime = new Date();
		logger1.info("插入结束时间：" + df.format(endTime));
		logger2.info("插入结束时间：" + df.format(endTime));
		Long timeDiff = endTime.getTime() - startTime.getTime();
		logger1.info("用时："+ timeDiff/1000+"秒");
		logger2.info("用时："+ timeDiff/1000+"秒");
		int diffNum = 0;
		do {
			try {
				diffNum = compareDateCount();
				if (diffNum!=0) {
					TimeUnit.SECONDS.sleep(10);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (diffNum!=0);

		logger2.info("最终同步结束时间为："+df.format(new Date()));
		Long timeDiff2 = new Date().getTime() - endTime.getTime();
		logger2.info("距离插入结束时间，同步时间差为："+ timeDiff2/1000+"秒。");
	}
	//创建定时查询的线程任务。
    public static void execTimerTask() {
        timer = new Timer();
        timerTask = new TimerTask() {
        	int timers = 0;
            public void run() {
            	int diffNum = compareDateCount(); 
        		double diff = Math.round((float)diffNum/(float)masterCount*10000)/100.0;
        		logger2.info("执行第"+(timers+1)+"次，第"+timers+"分钟的结果记录如下：");
        		logger2.info("主库数量："+masterCount+",备库数量为："+slaveCount+",主从库相差的数量为:"+diffNum+",占比为："+diff+"%");
        		timers++;
            }
        };
    timer.scheduleAtFixedRate(timerTask, 1000, 60000);
    }
    private static int compareDateCount() {
    	ExecutorService exec = Executors.newCachedThreadPool();  
    	Future<Integer> futureMaster = exec.submit(queryMaster);
    	Future<Integer> futureSlave = exec.submit(querySlave);
    	exec.shutdown();
    	try {
    		// 请求关闭、发生超时或者当前线程中断，无论哪一个首先发生之后，都将导致阻塞，直到所有任务完成执行
    		// 设置最长等待10秒
    		exec.awaitTermination(30, TimeUnit.SECONDS);
    	} catch (InterruptedException e) {
    		e.printStackTrace();
    		logger2.error("查询超时，不比对系统继续运行。"); 
    		return 0;
    	}
    	try {
    		masterCount = futureMaster.get();
    		slaveCount = futureSlave.get();
    	} catch (InterruptedException e) {
    		e.printStackTrace();
    	} catch (ExecutionException e) {
    		e.printStackTrace();
    	}
    	int diffNum = masterCount - slaveCount;
    	double diff = Math.round((float)diffNum/(float)masterCount*10000)/100.0;
    	logger1.info("主库数量："+masterCount+",备库数量为："+slaveCount+",主从库相差的数量为:"+diffNum+",占比为："+diff+"%");
    	logger2.info("主库数量："+masterCount+",备库数量为："+slaveCount+",主从库相差的数量为:"+diffNum+",占比为："+diff+"%");
    	return diffNum;
    }
}
