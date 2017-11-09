package com.kettle.record.remote;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.KettleRemotePool;

public class RemoteRecordProcess {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteRecordProcess.class);
	/**
	 * 远程池
	 */
	private List<RemoteRecordHandler> handlers;

	/**
	 * 定时任务
	 */
	private ScheduledExecutorService threadPool;

	/**
	 * 数据库
	 */
	private KettleDBClient dbClient;

	/**
	 * @param recordPool
	 * @param remotePool
	 */
	public RemoteRecordProcess(KettleDBClient dbClient) {
		this.handlers = new LinkedList<RemoteRecordHandler>();
		this.dbClient = dbClient;
	}

	/**
	 * 初始化
	 * 
	 * @param remotePool
	 */
	public void start(KettleRemotePool remotePool, KettleRecordPool recordPool) {
		this.threadPool = Executors.newScheduledThreadPool(20);
		Collection<KettleRemoteClient> clients = remotePool.getRemoteclients();
		for (KettleRemoteClient client : clients) {
			for (int index = 0; index < KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE; index++) {
				handlers.add(new RemoteRecordHandler(client));
			}
		}
	}

	/**
	 * 远端记录进程
	 * 
	 * @author Administrator
	 *
	 */
	private class RemoteRecordDaemon implements Runnable {

		private final KettleRemoteClient client;

		private final KettleRecordPool recordPool;

		private final List<KettleRecord> updateRecords = new LinkedList<KettleRecord>();

		private final KettleRecord[] recordArr;

		public RemoteRecordDaemon(KettleRemoteClient client, KettleRecordPool recordPool) {
			this.client = client;
			this.recordPool = recordPool;
			recordArr = new KettleRecord[client.maxRecord];
		}

		@Override
		public void run() {
			logger.debug("Kettle远端[" + client.getHostName() + "]定时任务轮询启动!");
			updateRecords.clear();
			if (client.isRunning()) {
				// 将Null排到最后
				Arrays.sort(recordArr, new Comparator<KettleRecord>() {
					public int compare(KettleRecord o1, KettleRecord o2) {
						if (o1 == null) {
							return 1;
						}
						if (o2 == null) {
							return -1;
						}
						return 0;
					}
				});
				for (int i = 0; i < recordArr.length; i++) {
					if (recordArr[i] != null) {
						dealRunningRecord(recordArr[i]);
						if (recordArr[i].isError() || recordArr[i].isFinished()) {
							updateRecords.add(recordArr[i]);
							recordArr[i] = null;
						}
					}
					// 尝试获取
					if (recordArr[i] == null) {
						recordArr[i] = recordPool.nextRecord();
					}
					// 申请状态
					if (recordArr[i] != null) {
						dealNotSendRecord(recordArr[i]);
					} else {
						// 如果没有任务,则直接下一步
						break;
					}
				}
			} else {
				for (int i = 0; i < recordArr.length; i++) {
					if (recordArr[i] != null) {
						dealErrorRemoteRecord(recordArr[i]);
						if (recordArr[i].isError() || recordArr[i].isFinished()) {
							updateRecords.add(recordArr[i]);
							recordArr[i] = null;
						}
					}
				}
			}
			if (!updateRecords.isEmpty()) {
				cleanRecords();
			}
			logger.debug("Kettle远端[" + client.getHostName() + "]定时任务轮询完成!");
		}

		/**
		 * @param 清理
		 */
		private void cleanRecords() {
			for (KettleRecord roll : updateRecords) {
				// 完成的进行清理
				if (roll.isFinished()) {
					removeJobNE(roll);
				}
			}
		}

		/**
		 * 清理
		 * 
		 * @param record
		 */
		private void removeJobNE(KettleRecord record) {
			try {
				client.remoteRemoveJob(record);
			} catch (Exception ex) {
				logger.debug("Kettle远端[" + client.getHostName() + "]清理record[" + record.getUuid() + "]失败!");
			}
		}

		/**
		 * 在远端无法连接时,所有运行中的任务为异常,Apply任务进入Record池
		 */
		private void dealErrorRemoteRecord(KettleRecord record) {
			if (record.isApply()) {
				recordPool.addPrioritizeRecord(record);
			} else if (record.isRunning()) {
				record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			}
		}

		/**
		 * 处理运行中的任务
		 *
		 * @param roll
		 */
		private void dealRunningRecord(KettleRecord job) {
			if (job.isRunning()) {
				String status = null;
				try {
					client.remoteJobStatus(job);
				} catch (Exception e) {
					logger.error("Kettle远端[" + client.getHostName() + "]查询Record[" + job.getUuid() + "]发生异常\n", e);
					status = KettleVariables.RECORD_STATUS_ERROR;
				}
				job.setStatus(status);
				checkJobRunOvertime(job);
				if (job.isError() || job.isFinished()) {
					try {
						dbClient.updateRecord(job);
					} catch (KettleException e) {
						logger.error("Kettle更新Record[" + job.getUuid() + "]状态[" + roll.getStatus() + "]发生异常\n", e);
					}
				}
			}
		}

		/**
		 * 是否超时
		 *
		 * @param job
		 */
		private void checkJobRunOvertime(KettleRecord job) {
			if (job.isRunning() && recordRunningMax != null && recordRunningMax > 0) {
				if ((System.currentTimeMillis() - job.getUpdateTime().getTime()) / 1000 / 60 > recordRunningMax) {
					remoteStopJobNE(job);
					job.setStatus(KettleVariables.RECORD_STATUS_ERROR);
					job.setErrMsg("Record[" + job.getUuid() + "]执行超时,异常状态!");
				}
			}
		}

		/**
		 * 处理未远程推送的任务
		 *
		 * @param roll
		 */
		private void dealNotSendRecord(KettleRecord job) {
			if (job.isApply() || job.isRepeat()) {
				try {
					String runID = client.remoteSendJob(job);
					job.setRunID(runID);
					job.setHostname(client.getHostName());
					job.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
					updateRecords.add(job);
				} catch (Exception e) {
					logger.error("Kettle远端[" + client.getHostName() + "]发送Record[" + job.getUuid() + "]发生异常\n", e);
					job.setStatus(KettleVariables.REMOTE_STATUS_ERROR);
					job.setErrMsg("Kettle远端[" + client.getHostName() + "]发送Record[" + job.getUuid() + "]发生异常");
					job.setHostname(client.getHostName());
					updateRecords.add(job);
				}
			}
		}
	}

}
