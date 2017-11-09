package com.kettle.remote;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.www.SlaveServerJobStatus;
import org.pentaho.di.www.SlaveServerStatus;
import org.pentaho.di.www.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleEnvDefault;
import com.kettle.core.KettleVariables;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;

/**
 * Kettle远程连接
 * 
 * @author chenkw
 *
 */
public class KettleRemoteClient {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	/**
	 * 资源链接
	 */
	private final KettleRepositoryClient repositoryClient;

	/**
	 * 远端状态 -- 初始状态未运行中
	 */
	private String remoteStatus = KettleVariables.REMOTE_STATUS_RUNNING;

	/**
	 * 远程服务
	 */
	private final SlaveServer remoteServer;

	/**
	 * 最大任务数量
	 */
	public final int maxRecord;

	public KettleRemoteClient(KettleRepositoryClient repositoryClient, final SlaveServer remoteServer)
			throws KettleException {
		this.repositoryClient = repositoryClient;
		this.remoteServer = remoteServer;
		maxRecord = KettleMgrEnvironment.NVLInt("KETTLE_RECORD_MAX_PER_REMOTE_" + remoteServer.getName(),
				KettleEnvDefault.KETTLE_RECORD_MAX_PER_REMOTE);
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
	 * 刷新状态
	 */
	public void refreshRemoteStatus() {
		remoteStatus = fetchRemoteStatus();
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
	 * @param job
	 *            job
	 * @return
	 * @throws KettleException
	 */
	public String remoteSendJob(KettleRecord job) throws KettleException {
		JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
		jobExecutionConfiguration.setRemoteServer(remoteServer);
		jobExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		jobExecutionConfiguration.setPassingExport(false);
		jobExecutionConfiguration.setExecutingRemotely(true);
		jobExecutionConfiguration.setExecutingLocally(false);
		jobExecutionConfiguration.setRepository(repositoryClient.getRepository());
		String runID = null;
		runID = Job.sendToSlaveServer(job.getKettleMeta(), jobExecutionConfiguration, repositoryClient.getRepository(),
				repositoryClient.getRepository().getMetaStore());
		return runID;
	}

	/**
	 * 远程启动,
	 * 
	 * @param job
	 *            job
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStartJob(KettleRecord job) throws Exception {
		WebResult result = remoteServer.startJob(job.getName(), job.getRunID());
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("Kettle远端[" + this.getHostName() + "]启动Job[" + job.getUuid() + "]失败!");
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
	public void remoteStopJobNE(KettleRecord job) {
		try {
			WebResult result = remoteServer.stopJob(job.getName(), job.getRunID());
			if (!"OK".equals(result.getResult())) {
				logger.debug("Kettle远端[" + this.getHostName() + "]停止Job[" + job.getUuid() + "]失败!");
			}
		} catch (Exception ex) {
			logger.debug("Kettle远端[" + this.getHostName() + "]停止Job[" + job.getUuid() + "]失败!");
		}
	}

	/**
	 * 获取远端的状态
	 * 
	 * @param job
	 * @return
	 * @throws Exception
	 */
	public void remoteJobStatus(KettleRecord job) throws Exception {
		SlaveServerJobStatus jobStatus = remoteServer.getJobStatus(job.getName(), job.getRunID(), 0);
		logger.debug("Kettle Remote[" + remoteServer.getHostname() + "]转换[" + job.getUuid() + "]状态为:"
				+ jobStatus.getStatusDescription());
		if (jobStatus == null || jobStatus.getStatusDescription() == null) {
			job.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			job.setErrMsg("remote[" + this.getHostName() + "]未找到record[" + job.getUuid() + "]信息!");
		} else if (jobStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			job.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			job.setErrMsg(jobStatus.getStatusDescription());
		} else if ("Finished".equalsIgnoreCase(jobStatus.getStatusDescription())) {
			job.setStatus(KettleVariables.RECORD_STATUS_FINISHED);
		} else {
			job.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		}
	}

	/**
	 * 清理运行的Trans
	 * 
	 * @param job
	 *            job
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteRemoveJob(KettleRecord job) throws Exception {
		WebResult result = remoteServer.removeJob(job.getName(), job.getRunID());
		if (!"OK".equals(result.getResult())) {
			logger.debug("Kettle远端[" + this.getHostName() + "]删除Job[" + job.getUuid() + "]失败!");
		}
	}

	/**
	 * 获取HostName
	 * 
	 * @return
	 */
	public String getHostName() {
		return this.remoteServer.getHostname();
	}
}
