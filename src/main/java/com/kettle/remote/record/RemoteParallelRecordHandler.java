package com.kettle.remote.record;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrEnvironment;
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
public class RemoteParallelRecordHandler {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteParallelRecordHandler.class);

	/**
	 * 远端
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * records,存放改Remote处理的Record,即Record的Hostname属性为该remote
	 */
	private List<KettleRecord> thisRemoteRecords = new LinkedList<KettleRecord>();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool;

	/**
	 * 并行线程
	 */
	private RecordOperatorThread[] remoteRecordThreads;

	/**
	 * @param remoteClient
	 * @param oldRecords
	 */
	public RemoteParallelRecordHandler(KettleRemoteClient remoteClient, List<KettleRecord> oldRecords) {
		threadPool = Executors.newScheduledThreadPool(KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE);
		remoteRecordThreads = new RecordOperatorThread[KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE];
		for (int i = 0; i < remoteRecordThreads.length; i++) {
			remoteRecordThreads[i] = new RecordOperatorThread();
			remoteRecordThreads[i].remoteRecordOperator = new RemoteRecordOperator(remoteClient);
		}
		this.remoteClient = remoteClient;
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		if (oldRecords != null && !oldRecords.isEmpty()) {
			thisRemoteRecords.addAll(oldRecords);
		}
		tryAwaken();
	}

	/**
	 * 尝试唤醒
	 */
	public void tryAwaken() {
		if (!remoteClient.isRunning()) {
			return;
		}
		if (thisRemoteRecords.isEmpty()) {
			attackRecords();
		}
	}

	/**
	 * 尝试停止
	 * 
	 * @param record
	 * @return 是否成功
	 */
	@SuppressWarnings("deprecation")
	public boolean tryRemoveRecord(KettleRecord record) {
		KettleRecord remoteRecord = null;
		if (record.getHostname() != null && !remoteClient.getHostName().equals(record.getHostname())) {
			return false;
		}
		for (RecordOperatorThread thread : remoteRecordThreads) {
			remoteRecord = thread.remoteRecordOperator.getRecord();
			if (thread.isRunning && remoteRecord.getUuid().equals(record.getUuid())) {
				thread.stop();
				if (remoteClient.isRunning()) {
					remoteClient.remoteStopJobNE(remoteRecord);
					remoteClient.remoteRemoveJobNE(remoteRecord);
				}
				thread.remoteRecordOperator.detachRecord();
				thread.isRunning = false;
				return true;
			}
		}
		return false;
	}

	/**
	 * 获取记录
	 * 
	 * @return
	 */
	private synchronized void attackRecords() {
		KettleRecord recordTMP = null;
		for (RecordOperatorThread operatorThread : remoteRecordThreads) {
			if (!operatorThread.isRunning) {
				recordTMP = getNextRecord();
				if (recordTMP == null) {
					return;
				}
				if (operatorThread.remoteRecordOperator.attachRecord(recordTMP)) {
					logger.info("remote[" + remoteClient.getHostName() + "]开始处理Record[" + recordTMP.getUuid() + "]!");
					operatorThread.isRunning = true;
					threadPool.scheduleAtFixedRate(operatorThread, 1, 5, TimeUnit.SECONDS);
				} else {
					callBackRecord(recordTMP);
					break;
				}
			}
		}
	}

	/**
	 * 获取下一个任务
	 * 
	 * @return
	 */
	private synchronized KettleRecord getNextRecord() {
		if (thisRemoteRecords.isEmpty()) {
			return recordPool.nextRecord();
		}
		return thisRemoteRecords.remove(0);
	}

	/**
	 * 获取下一个任务
	 * @param record
	 * @return
	 */
	private synchronized void callBackRecord(KettleRecord record) {
		if (record == null) {
			return;
		}
		if (record.getHostname() != null) {
			thisRemoteRecords.add(record);
		} else {
			recordPool.addPrioritizeRecord(record);
		}
	}

	/**
	 * @author Administrator
	 *
	 */
	private class RecordOperatorThread extends Thread {

		private boolean isRunning = false;

		private RemoteRecordOperator remoteRecordOperator;

		@SuppressWarnings("deprecation")
		@Override
		public void run() {
			isRunning = true;
			try {
				remoteRecordOperator.dealRecord();
				if (remoteRecordOperator.isFinished()) {
					remoteRecordOperator.detachRecord();
					KettleRecord recordTMP = getNextRecord();
					if (recordTMP == null) { // 如果没有后续任务,直接停止进程
						isRunning = false;
						this.stop();
						return;
					}
					if (remoteRecordOperator.attachRecord(recordTMP)) {
						logger.info("remote[" + remoteClient.getHostName() + "]开始处理下一个Record[" + recordTMP.getUuid()
								+ "]!");
						remoteRecordOperator.dealRecord();
					} else {
						callBackRecord(recordTMP);
						isRunning = false;
						this.stop();
					}
				}
			} catch (Exception e) {
				logger.error("remote[" + remoteClient.getHostName() + "]处理进程发生错误!", e);
			}
		}

	}
}
