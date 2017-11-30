package com.kettle.remote.record;

import java.util.LinkedList;
import java.util.List;

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
	 * 保存遗留的任务
	 */
	private List<KettleRecord> oldKettleRecords = new LinkedList<KettleRecord>();

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
	 * 保存遗留数据
	 */
	public void addOldKettleRecord(KettleRecord kettleRecord) {
		oldKettleRecords.add(kettleRecord);
	}

	/**
	 * 获取下一个记录
	 * 
	 * @return
	 */
	private synchronized KettleRecord getNextRecord() {
		if (oldKettleRecords.isEmpty()) {
			return recordPool.nextRecord();
		} else {
			return oldKettleRecords.remove(0);
		}
	}

	/**
	 * 回退Record
	 * 
	 * @return
	 */
	private synchronized void callBackRecord(KettleRecord record) {
		if (record != null) {
			if (record.getHostname() != null) {
				oldKettleRecords.add(record);
			} else {
				recordPool.addPrioritizeRecord(record);
			}
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() {
		isRunning = true;
		logger.debug("Kettle远端进程[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]进程唤醒!");
		try {
			KettleRecord recordTMP = null;
			// 如果已经加载,直接进行任务处理
			if (remoteRecordOperator.isAttached()) {
				// 进行处理
				remoteRecordOperator.dealRecord();
				if (remoteRecordOperator.isFinished()) {
					remoteRecordOperator.detachRecord();
				}
			} else {
				recordTMP = getNextRecord();
				if (recordTMP == null) {
					// 停止进程
					Thread.currentThread().stop();
					isRunning = false;
				}
			}
			// 尝试自动加载任务
			if (!remoteRecordOperator.isAttached()) {
				// 自动加载任务
				if (remoteRecordOperator.attachRecord(recordTMP)) {
					remoteRecordOperator.dealRecord();
				} else {
					callBackRecord(recordTMP);
				}
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO
					+ "]定时任务发生异常成!", ex);
		}
		logger.debug(
				"Kettle远端[" + remoteRecordOperator.getRemoteClient().getHostName() + "-" + processNO + "]定时任务轮询完成!");
	}
}
