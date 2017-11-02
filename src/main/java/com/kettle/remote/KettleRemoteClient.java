package com.kettle.remote;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerStatus;
import org.pentaho.di.www.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.repo.KettleDBRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordPool;

public class KettleRemoteClient {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	/**
	 * 远程执行锁
	 */
	private static Object remoteLock = new Object();

	/**
	 * 远程池
	 */
	private final KettleRemotePool kettleRemotePool;

	/**
	 * 资源链接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	/**
	 * 远端状态
	 */
	private String remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;

	/**
	 * 远程服务
	 */
	private final SlaveServer remoteServer;

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool;

	/**
	 * 查看任务状态的线程
	 */
	private final KettleRecord[] recordArr;

	/**
	 * 任务池
	 */
	private final KettleRecordPool kettleRecordPool;

	public KettleRemoteClient(KettleRemotePool kettleRemotePool, final SlaveServer remoteServer, int initialDelay)
			throws KettleException {
		this.kettleRemotePool = kettleRemotePool;
		this.remoteServer = remoteServer;
		this.dbRepositoryClient = kettleRemotePool.getDbRepositoryClient();
		this.kettleRecordPool = kettleRemotePool.getKettleRecordPool();
		int maxRecord = 6;
		threadPool = Executors.newSingleThreadScheduledExecutor();
		recordArr = new KettleRecord[maxRecord];
		threadPool.execute(new Runnable() {
			@Override
			public void run() {
				checkRemoteStatus();
				logger.debug("Kettle远端[" + getHostName() + "]初始化检测状态:" + remoteStatus);
			}
		});
		threadPool.scheduleAtFixedRate(new RemoteRecordDaemon(), initialDelay, 20, TimeUnit.SECONDS);
	}

	/**
	 * 远端状态
	 * 
	 * @return
	 */
	private String fetchRemoteStatus() {
		try {
			SlaveServerStatus status = remoteServer.getStatus();
			logger.debug("Kettle远端[" + getHostName() + "]状态:" + status.getStatusDescription());
			return status.getStatusDescription();
		} catch (Exception e) {
			logger.error("Kettle远端[" + getHostName() + "]查看状态发生异常\n", e);
			return KettleVariables.REMOTE_STATUS_ERROR;
		}
	}

	/**
	 * 验证状态
	 * 
	 * @return true 正常;false 非正常
	 */
	private boolean checkRemoteStatus() {
		if (KettleVariables.REMOTE_STATUS_ERROR.equals(remoteStatus)) {
			return false;
		} else if (KettleVariables.REMOTE_STATUS_RUNNING.equals(fetchRemoteStatus())) {
			remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;
			return true;
		} else {
			remoteStatus = KettleVariables.REMOTE_STATUS_ERROR;
			return false;
		}
	}

	/**
	 * 恢复状态
	 * 
	 * @return 将状态设置为正常状态
	 */
	private void recoveryStatus() {
		if (KettleVariables.REMOTE_STATUS_ERROR.equals(fetchRemoteStatus())) {
			remoteStatus = KettleVariables.REMOTE_STATUS_ERROR;
		} else {
			remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;
		}
	}

	/**
	 * 是否运行状态
	 * 
	 * @return
	 */
	public boolean isRunning() {
		return KettleVariables.REMOTE_STATUS_RUNNING.equals(remoteStatus);
	}

	/**
	 * 远程推送Job
	 * 
	 * @param jobMeta
	 * @return
	 * @throws KettleException
	 */
	public String remoteSendJob(JobMeta jobMeta) throws KettleException {
		JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
		jobExecutionConfiguration.setRemoteServer(remoteServer);
		jobExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		jobExecutionConfiguration.setPassingExport(false);
		jobExecutionConfiguration.setExecutingRemotely(true);
		jobExecutionConfiguration.setExecutingLocally(false);
		jobExecutionConfiguration.setRepository(kettleRemotePool.getDbRepository());
		String runID = null;
		synchronized (remoteLock) {
			runID = Job.sendToSlaveServer(jobMeta, jobExecutionConfiguration, kettleRemotePool.getDbRepository(),
					kettleRemotePool.getDbRepository().getMetaStore());
		}
		return runID;
	}

	/**
	 * 远程启动,
	 * 
	 * @param jobName
	 *            必填
	 * @param runid
	 *            选填
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStartJob(String jobName, String runid) throws KettleException, Exception {
		WebResult result = remoteServer.startJob(jobName, runid);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("Kettle远端[" + this.getHostName() + "]启动Job[" + jobName + "]失败!");
		}
	}

	/**
	 * 远程停止任务
	 * 
	 * @param jobName
	 * @param runid
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStopJob(String jobName, String runid) throws KettleException, Exception {
		WebResult result = remoteServer.stopJob(jobName, runid);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("工作[" + jobName + "]停止失败!");
		}
	}

	/**
	 * 获取远端的状态
	 * 
	 * @param jobname
	 * @return
	 * @throws Exception
	 */
	public String remoteJobStatus(String jobname) throws Exception {
		SlaveServerJobStatus jobStatus = remoteServer.getJobStatus(jobname, "", 0);
		logger.debug("Kettle Remote[" + remoteServer.getHostname() + "]转换[" + jobname + "]状态为:"
				+ jobStatus.getStatusDescription());
		if (jobStatus == null || jobStatus.getStatusDescription() == null) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if (jobStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if ("Finished".equalsIgnoreCase(jobStatus.getStatusDescription())) {
			return KettleVariables.RECORD_STATUS_FINISHED;
		} else {
			return KettleVariables.RECORD_STATUS_RUNNING;
		}
	}

	/**
	 * 清理运行的Trans
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteRemoveJob(String jobName, String runid) throws KettleException, Exception {
		remoteServer.removeJob(jobName, runid);
	}

	/**
	 * 获取HostName
	 * 
	 * @return
	 */
	public String getHostName() {
		return this.remoteServer.getHostname();
	}

	/**
	 * 空闲数量
	 * 
	 * @return
	 */
	public int freeDaemonCount() {
		int size = 0;
		for (KettleRecord roll : recordArr) {
			if (roll == null) {
				size++;
			}
		}
		return size;
	}

	/**
	 * 远端记录进程
	 * 
	 * @author Administrator
	 *
	 */
	private class RemoteRecordDaemon implements Runnable {

		private final List<KettleRecord> updateRecords = new LinkedList<KettleRecord>();

		@Override
		public void run() {
			logger.debug("Kettle远端[" + getHostName() + "]定时任务轮询启动!");
			updateRecords.clear();
			if (checkRemoteStatus()) {
				for (int i = 0; i < recordArr.length; i++) {
					if (recordArr[i] != null) {
						dealRunningRecord(recordArr[i]);
						if (recordArr[i].isError() || recordArr[i].isFinished()) {
							updateRecords.add(recordArr[i]);
							recordArr[i] = null;
						}
					}
					/*
					 * 尝试获取
					 */
					if (recordArr[i] == null) {
						recordArr[i] = kettleRecordPool.nextRecord();
					}
					/*
					 * 申请状态
					 */
					if (recordArr[i] != null) {
						dealNotSendRecord(recordArr[i]);
					}
				}
			} else {
				for (int i = 0; i < recordArr.length; i++) {
					if (recordArr[i] != null) {
						dealRemoteErrorRecord(recordArr[i]);
						if (recordArr[i].isError() || recordArr[i].isFinished()) {
							updateRecords.add(recordArr[i]);
							recordArr[i] = null;
						}
					}
				}
				recoveryStatus();
			}
			if (!updateRecords.isEmpty()) {
				try {
					dbRepositoryClient.updateRecords(updateRecords);
					cleanRecords();
				} catch (KettleException e) {
					logger.error("Record" + updateRecords + "更新记录发生异常", e);
				}
			}
			logger.debug("Kettle远端[" + getHostName() + "]定时任务轮询完成!");
		}

		/**
		 * @param 清理
		 */
		private void cleanRecords() {
			for (KettleRecord roll : updateRecords) {
				if (roll.isError() || roll.isFinished()) {
					kettleRecordPool.deleteRecord(roll.getId());
				}
				// 完成的进行清理
				if (roll.isFinished()) {
					if (KettleRecord.class.isInstance(roll)) {
						KettleRecord job = (KettleRecord) roll;
						try {
							remoteRemoveJob(job.getName(), null);
						} catch (Exception e) {
							logger.error("Kettle远端[" + getHostName() + "]清理Job[" + job.getId() + "]发生异常", e);
						}
					}
				}
			}
		}

		/**
		 * 处理远端的异常情况
		 */
		private void dealRemoteErrorRecord(KettleRecord record) {
			if (record.isApply()) {
				kettleRecordPool.addPrioritizeRecord(record);
			} else if (record.isRunning()) {
				record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			}
		}

		/**
		 * 处理运行中的任务
		 * 
		 * @param roll
		 */
		private void dealRunningRecord(KettleRecord roll) {
			if (roll.isRunning()) {
				String status = null;
				if (KettleRecord.class.isInstance(roll)) {
					KettleRecord job = (KettleRecord) roll;
					try {
						status = remoteJobStatus(job.getName());
					} catch (Exception e) {
						logger.error("Kettle远端[" + getHostName() + "]查询Job[" + job.getId() + "]发生异常\n", e);
						status = KettleVariables.RECORD_STATUS_ERROR;
					}
					job.setStatus(status);
				}
			}
		}

		/**
		 * 处理未远程推送的任务
		 * 
		 * @param roll
		 */
		private void dealNotSendRecord(KettleRecord roll) {
			if (roll.isApply() || roll.isRepeat()) {
				KettleRecord job = (KettleRecord) roll;
				try {
					if (job.getKettleMeta() == null) {
						job.setKettleMeta(dbRepositoryClient.getJobMeta(job.getId()));
					}
					String runID = remoteSendJob(job.getKettleMeta());
					roll.setRunID(runID);
					roll.setHostname(getHostName());
					roll.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
					updateRecords.add(roll);
				} catch (Exception e) {
					logger.error("Kettle远端[" + getHostName() + "]发送Job[" + job.getId() + "]发生异常\n", e);
					roll.setStatus(KettleVariables.REMOTE_STATUS_ERROR);
					roll.setErrMsg("Kettle远端[" + getHostName() + "]发送Job[" + job.getId() + "]发生异常");
					roll.setHostname(getHostName());
					updateRecords.add(job);
				}
			}
		}
	}
}
