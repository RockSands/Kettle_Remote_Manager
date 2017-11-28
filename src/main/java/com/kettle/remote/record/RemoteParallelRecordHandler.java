package com.kettle.remote.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

/**
 * 远程处理任务
 * 
 * @author Administrator
 *
 */
public class RemoteParallelRecordHandler implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(RemoteParallelRecordHandler.class);

	private final int processNO;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * 处理的任务
	 */
	private final RemoteRecordOperator remoteRecordOperator;

	/**
	 * @param remoteClient
	 */
	public RemoteParallelRecordHandler(int processNO, KettleRemoteClient remoteClient) {
		this.processNO = processNO;
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		remoteRecordOperator = new RemoteRecordOperator(remoteClient);
	}

	@Override
	public void run() {
		logger.debug(
				"Kettle远端进程[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]定时任务轮询启动!");
		try {
			if (remoteRecordOperator.isFree()) {
				remoteRecordOperator.attachRecord(recordPool.nextRecord());
			}
			if (!remoteRecordOperator.isFree()) {// 如果未加载成功
				remoteRecordOperator.dealRecord();
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO
					+ "]定时任务发生异常成!", ex);
		}
		logger.debug(
				"Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]定时任务轮询完成!");
	}
}
