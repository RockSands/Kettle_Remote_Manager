package com.kettle.remote.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;
import com.kettle.record.pool.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

/**
 * 远程处理任务,并发单元
 * 
 * @author Administrator
 *
 */
public class RemoteParallelRecordHandler implements Runnable {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteParallelRecordHandler.class);

	/**
	 * 进程号
	 */
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
	 * 是否运行中
	 */
	private boolean isRunning = false;

	/**
	 * @param remoteClient
	 */
	public RemoteParallelRecordHandler(int processNO, KettleRemoteClient remoteClient) {
		this.processNO = processNO;
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		remoteRecordOperator = new RemoteRecordOperator(remoteClient);
	}

	public boolean isRunning() {
		return isRunning;
	}

	/**
	 * 匹配
	 * 
	 * @param hostName
	 * @return
	 */
	public boolean match(String hostName) {
		return hostName == null || remoteRecordOperator.getRemoteClient().getHostName().equals(hostName);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		isRunning = true;
		logger.debug("Kettle远端进程[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]进程唤醒!");
		try {
			// 如果已经加载,直接进行任务处理
			if (remoteRecordOperator.isAttached()) {
				// 进行处理
				remoteRecordOperator.dealRecord();
				if (remoteRecordOperator.isFinished()) {
					remoteRecordOperator.detachRecord();
				}
			}
			// 尝试自动加载任务
			if (!remoteRecordOperator.isAttached()) {
				// 自动加载任务
				KettleRecord recordTMP = recordPool.nextRecord();
				if (recordTMP != null) {
					if (remoteRecordOperator.attachRecord(recordTMP)) {
						remoteRecordOperator.dealRecord();
					} else {
						recordPool.addPrioritizeRecord(recordTMP);
					}
				}
			}
			// 如果加载不到Record,直接关闭此线程
			if (!remoteRecordOperator.isAttached()) {
				// 停止进程
				Thread.currentThread().stop();
				isRunning = false;
			}

		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO
					+ "]定时任务发生异常成!", ex);
		}
		logger.debug(
				"Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]定时任务轮询完成!");
	}
}
