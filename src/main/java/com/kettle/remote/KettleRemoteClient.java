package com.kettle.remote;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.repo.KettleDBRepositoryClient;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordPool;
import com.kettle.record.KettleTransRecord;

public class KettleRemoteClient {

	Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

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
	private String remoteStatus = null;

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
	private final KettleRecordPool kettleRecordPool = new KettleRecordPool();

	public KettleRemoteClient(KettleRemotePool kettleRemotePool, final SlaveServer remoteServer)
			throws KettleException {
		this.kettleRemotePool = kettleRemotePool;
		this.remoteServer = remoteServer;
		this.dbRepositoryClient = kettleRemotePool.getDbRepositoryClient();
		int maxRecord = 6;
		threadPool = Executors.newScheduledThreadPool(maxRecord);
		recordArr = new KettleRecord[maxRecord];
		threadPool.scheduleAtFixedRate(new RemoteRecordDaemon(), 10, 20, TimeUnit.SECONDS);
	}

	/**
	 * 查看远端状态
	 * 
	 * @return
	 */
	private String getRemoteStatus() {
		try {
			SlaveServerStatus status = remoteServer.getStatus();
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
	public boolean checkRemoteStatus() {
		synchronized (remoteStatus) {
			if (KettleVariables.REMOTE_STATUS_ERROR.equals(remoteStatus)) {
				return false;
			} else if (KettleVariables.REMOTE_STATUS_ERROR.equals(getRemoteStatus())) {
				remoteStatus = KettleVariables.REMOTE_STATUS_ERROR;
				return false;
			} else {
				remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;
				return true;
			}
		}
	}

	/**
	 * 恢复状态
	 * 
	 * @return 将状态设置为正常状态
	 */
	public synchronized void recoveryStatus() {
		synchronized (remoteStatus) {
			if (KettleVariables.REMOTE_STATUS_ERROR.equals(getRemoteStatus())) {
				remoteStatus = KettleVariables.REMOTE_STATUS_ERROR;
			} else {
				remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;
			}
		}
	}

	/**
	 * 远程推送Trans
	 * 
	 * @param transMeta
	 * @throws KettleException
	 * @throws Exception
	 */
	public String remoteSendTrans(TransMeta transMeta) throws KettleException {
		TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
		transExecutionConfiguration.setRemoteServer(remoteServer);
		transExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		transExecutionConfiguration.setPassingExport(false);
		transExecutionConfiguration.setExecutingRemotely(true);
		transExecutionConfiguration.setExecutingLocally(false);
		String runID = Trans.sendToSlaveServer(transMeta, transExecutionConfiguration,
				kettleRemotePool.getDbRepository(), kettleRemotePool.getDbRepository().getMetaStore());
		return runID;
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
		String runid = Job.sendToSlaveServer(jobMeta, jobExecutionConfiguration, kettleRemotePool.getDbRepository(),
				kettleRemotePool.getDbRepository().getMetaStore());
		return runid;
	}

	/**
	 * 清理运行的Trans
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteCleanTrans(String transName, String runId) throws KettleException, Exception {
		remoteServer.cleanupTransformation(transName, null);
	}

	/**
	 * 远程启动, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStartTrans(String transName, String runId) throws KettleException {
		WebResult result = null;
		try {
			result = remoteServer.startTransformation(transName, runId);
		} catch (Exception e) {
			throw new KettleException("Kettle远端[" + getHostName() + "]启动Trans[" + transName + "]失败!");
		}
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("Kettle远端[" + getHostName() + "]启动Trans[" + transName + "]失败!");
		}
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
	 * 远程停止, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStopTrans(String transName) throws KettleException, Exception {
		WebResult result = remoteServer.stopTransformation(transName, null);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("转换[" + transName + "]停止失败!");
		}
	}

	public void remoteStopJob(String transName, String runid) throws KettleException, Exception {
		WebResult result = remoteServer.stopJob(transName, runid);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("工作[" + transName + "]停止失败!");
		}
	}

	/**
	 * 远程查询状态
	 * 
	 * @param transName
	 * @return
	 * @throws KettleException
	 * @throws Exception
	 */
	public String remoteTransStatus(String transName) throws Exception {
		SlaveServerTransStatus transStatus = remoteServer.getTransStatus(transName, "", 0);
		logger.debug("Kettle Remote[" + remoteServer.getHostname() + "]转换[" + transName + "]状态为:"
				+ transStatus.getStatusDescription());
		if (transStatus == null || transStatus.getStatusDescription() == null) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if (transStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if ("Finished".equalsIgnoreCase(transStatus.getStatusDescription())) {
			return KettleVariables.RECORD_STATUS_FINISHED;
		} else {
			return KettleVariables.RECORD_STATUS_RUNNING;
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
	 * @param transName
	 * @param runid
	 * @throws Exception
	 */
	public void remoteRemoveTran(String transName, String runid) throws Exception {
		remoteServer.removeTransformation(transName, runid);
	}

	/**
	 * 清理运行的Trans
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteRemoveJob(String jobName) throws KettleException, Exception {
		remoteServer.removeJob(jobName, null);
	}

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
	 * 申请任务
	 */
	public void applyRecord() {
		KettleRecord record = kettleRecordPool.nextRecord();
		if (record != null) {
			synchronized (recordArr) {
				for (KettleRecord roll : recordArr) {
					if (roll == null) {
						roll = record;
					}
				}
			}
		}
	}

	/**
	 * 更新Job,无异常
	 * 
	 * @param job
	 */
	private void updateJobRecordNE(KettleJobRecord job) {
		try {
			dbRepositoryClient.updateJobRecord(job);
		} catch (Exception e) {
			logger.error("Job[" + job.getId() + "]更新记录发生异常", e);
		}
	}

	/**
	 * 更新Trans,无异常
	 * 
	 * @param job
	 */
	private void updateTransRecordNE(KettleTransRecord trans) {
		try {
			dbRepositoryClient.updateTransRecord(trans);
		} catch (Exception e) {
			logger.error("Trans[" + trans.getId() + "]更新记录发生异常", e);
		}
	}

	private class RemoteRecordDaemon implements Runnable {

		@Override
		public void run() {
			if (checkRemoteStatus()) {
				for (KettleRecord roll : recordArr) {
					if (roll != null) {
						// 处理运行中的任务
						dealRunningRecord(roll);
						if (roll.isError() || roll.isFinished()) {
							roll = kettleRecordPool.nextRecord();
						}
						// 处理申请状态的Record
						dealApplyRecord(roll);
					} else {
						roll = kettleRecordPool.nextRecord();
						dealApplyRecord(roll);
					}
				}
			} else {
				recoveryStatus();
			}
		}

		private void dealRunningRecord(KettleRecord roll) {
			if (roll.isRunning()) {
				String status = null;
				if (KettleJobRecord.class.isInstance(roll)) {
					KettleJobRecord job = (KettleJobRecord) roll;
					try {
						status = remoteTransStatus(job.getKettleMeta().getName());
					} catch (Exception e) {
						logger.error("Kettle远端[" + getHostName() + "]查询Job[" + job.getId() + "]发生异常\n", e);
						status = KettleVariables.RECORD_STATUS_ERROR;
					}
					job.setStatus(status);
					if (job.isError() || job.isFinished()) {
						updateJobRecordNE(job);
					}
				}
				if (KettleTransRecord.class.isInstance(roll)) {
					KettleTransRecord trans = (KettleTransRecord) roll;
					try {
						status = remoteTransStatus(trans.getKettleMeta().getName());
					} catch (Exception e) {
						logger.error("Kettle远端[" + getHostName() + "]查询Trans[" + trans.getId() + "]发生异常\n", e);
						status = KettleVariables.RECORD_STATUS_ERROR;
					}
					trans.setStatus(status);
					if (trans.isError() || trans.isFinished()) {
						updateTransRecordNE(trans);
					}
				}
			}
		}

		private void dealApplyRecord(KettleRecord roll) {
			if (roll.isApply()) {
				if (KettleJobRecord.class.isInstance(roll)) {
					KettleJobRecord job = (KettleJobRecord) roll;
					try {
						String runID = remoteSendJob(job.getKettleMeta());
						job.setRunID(runID);
						job.setHostname(getHostName());
					} catch (Exception e) {
						logger.error("Kettle远端[" + getHostName() + "]发送Job[" + job.getId() + "]发生异常\n", e);
						job.setStatus(KettleVariables.REMOTE_STATUS_ERROR);
						job.setErrMsg("Kettle远端[" + getHostName() + "]发送Job[" + job.getId() + "]发生异常");
					}
					updateJobRecordNE(job);
				}
				if (KettleTransRecord.class.isInstance(roll)) {
					KettleTransRecord trans = (KettleTransRecord) roll;
					try {
						String runID = remoteSendTrans(trans.getKettleMeta());
						trans.setRunID(runID);
						trans.setHostname(getHostName());
					} catch (Exception e) {
						logger.error("Kettle远端[" + getHostName() + "]发送Trans[" + trans.getId() + "]发生异常\n", e);
						trans.setStatus(KettleVariables.REMOTE_STATUS_ERROR);
						trans.setErrMsg("Kettle远端[" + getHostName() + "]发送Trans[" + trans.getId() + "]发生异常");
					}
					updateTransRecordNE(trans);
				}
			}
		}

	}
}
