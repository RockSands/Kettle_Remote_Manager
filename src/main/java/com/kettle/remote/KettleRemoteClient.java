package com.kettle.remote;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.www.SlaveServerStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.repo.KettleDBRepositoryClient;

public class KettleRemoteClient {

	Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	private String remoteStatus = null;

	/**
	 * 远程服务
	 */
	private final SlaveServer remoteServer;

	/**
	 * 资源池数据库连接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	public KettleRemoteClient(final KettleDBRepositoryClient dbRepositoryClient, final SlaveServer remoteServer)
			throws KettleException {
		this.dbRepositoryClient = dbRepositoryClient;
		this.remoteServer = remoteServer;
		checkStatus();
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
	public boolean checkStatus() {
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
				dbRepositoryClient.getRepository(), dbRepositoryClient.getRepository().getMetaStore());
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
		String runid = Job.sendToSlaveServer(jobMeta, jobExecutionConfiguration, dbRepositoryClient.getRepository(),
				dbRepositoryClient.getRepository().getMetaStore());
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
	public String remoteTransStatus(String transName) throws KettleException, Exception {
		SlaveServerTransStatus slaveServerStatus = remoteServer.getTransStatus(transName, "", 0);
		System.out.println("Server[" + remoteServer.getHostname() + "]转换[" + transName + "]状态为:"
				+ slaveServerStatus.getStatusDescription());
		if (slaveServerStatus == null || slaveServerStatus.getStatusDescription() == null) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if (slaveServerStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			return KettleVariables.RECORD_STATUS_ERROR;
		} else if ("Finished".equalsIgnoreCase(slaveServerStatus.getStatusDescription())) {
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
}
