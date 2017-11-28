package com.kettle.remote.record;

import java.util.Arrays;
import java.util.Comparator;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleRecord;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

/**
 * Kettle远程任务处理,该处理以Remote为核心
 * 
 * @author Administrator
 *
 */
public class RemoteSerialRecordHandler implements Runnable {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteSerialRecordHandler.class);

	/**
	 * 远端
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * 数据库
	 */
	private final KettleDBClient dbClient;

	/**
	 * 处理的任务
	 */
	private final RemoteRecordOperator[] remoteRecordOperators;

	/**
	 * @param client
	 */
	public RemoteSerialRecordHandler(KettleRemoteClient remoteClient) {
		this.remoteClient = remoteClient;
		this.remoteRecordOperators = new RemoteRecordOperator[remoteClient.maxRecord];
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	}

	/**
	 * 排序,将Null放到最后
	 */
	private void sortRecords() {
		Arrays.sort(remoteRecordOperators, new Comparator<RemoteRecordOperator>() {
			public int compare(RemoteRecordOperator o1, RemoteRecordOperator o2) {
				if (o1.getRecord() == null) {
					return 1;
				}
				if (o2.getRecord() == null) {
					return -1;
				}
				return 0;
			}
		});
	}

	@Override
	public void run() {
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询启动!");
		try {
			sortRecords();
			for (RemoteRecordOperator recordOperator : remoteRecordOperators) {
				if (recordOperator.getRecord() == null && !recordOperator.attachRecord(recordPool.nextRecord())) {
					break;
				}
				if (recordOperator.getRecord() == null) {

				}
				recordOperator.dealRecord();
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]定时任务发生异常成!", ex);
		}
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询完成!");
	}
}
