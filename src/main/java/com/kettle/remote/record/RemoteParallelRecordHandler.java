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
import com.kettle.record.pool.KettleRecordPoolMonitor;
import com.kettle.remote.KettleRemoteClient;

/**
 * 远程处理任务,并发单元
 * 
 * @author Administrator
 *
 */
public class RemoteParallelRecordHandler implements KettleRecordPoolMonitor {

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
	 * records
	 */
	private List<KettleRecord> kettleRecords = new LinkedList<KettleRecord>();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool;

	/**
	 * 
	 */
	private RecordOperatorThread[] remoteRecordThreads;

	/**
	 * @param remoteClient
	 */
	public RemoteParallelRecordHandler(KettleRemoteClient remoteClient, List<KettleRecord> oldRecords) {
		threadPool = Executors.newScheduledThreadPool(KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE / 2);
		RecordOperatorThread[] remoteRecordThreads = new RecordOperatorThread[KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE];
		for (int i = 0; i < remoteRecordThreads.length; i++) {
			remoteRecordThreads[i] = new RecordOperatorThread();
		}
		this.remoteClient = remoteClient;
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		if (oldRecords != null && !oldRecords.isEmpty()) {
			kettleRecords.addAll(oldRecords);
		}
		recordPool.registePoolMonitor(this);
	}

	@Override
	public void addRecordNotify() {
		if (!remoteClient.isRunning()) {
			return;
		}
		if (kettleRecords.isEmpty()) {
			attackRecords();
		}
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
				operatorThread.remoteRecordOperator.attachRecord(recordTMP);
				if (!remoteClient.isRunning()) {// 远端无法连接
					kettleRecords.add(recordTMP);
					operatorThread.remoteRecordOperator.detachRecord();
					// 进行等待
					threadPool.schedule(new Runnable() {
						long startTime = System.currentTimeMillis();

						@SuppressWarnings("deprecation")
						@Override
						public void run() {
							if (!remoteClient.isRunning()) {
								for (KettleRecord index : kettleRecords) {
									if (index.isApply()) {
										recordPool.addPrioritizeRecord(index);
									}
								}
								if ((System.currentTimeMillis() - startTime) / 1000 / 60 > 60l) {// 一个小时后停止监听
									Thread.currentThread().stop();
								}
							} else {
								addRecordNotify();
							}
						}
					}, 30, TimeUnit.SECONDS);
					return;
				} else {// 启动
					logger.debug(
							"remote[" + remoteClient.getHostName() + "]唤醒一个处理进程处理Record[" + recordTMP.getUuid() + "]");
					threadPool.scheduleAtFixedRate(operatorThread, 2, 10, TimeUnit.SECONDS);
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
		if (kettleRecords.isEmpty()) {
			return recordPool.nextRecord();
		}
		return kettleRecords.remove(0);
	}

	/**
	 * @author Administrator
	 *
	 */
	private class RecordOperatorThread implements Runnable {

		private boolean isRunning = false;

		private final RemoteRecordOperator remoteRecordOperator = new RemoteRecordOperator(remoteClient);

		@SuppressWarnings("deprecation")
		@Override
		public void run() {
			isRunning = true;
			try {
				remoteRecordOperator.dealRecord();
				if (remoteRecordOperator.isFinished()) {
					remoteRecordOperator.detachRecord();
					KettleRecord recordTMP = getNextRecord();
					remoteRecordOperator.attachRecord(recordTMP);
					if (!remoteRecordOperator.isRunning()) {
						isRunning = false;
						Thread.currentThread().stop();
					}
				}
			} catch (Exception e) {
				logger.error("remote[" + remoteClient.getHostName() + "]处理进程发生错误!", e);
			}
		}

	}
}
