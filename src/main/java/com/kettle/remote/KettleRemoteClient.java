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

import com.kettle.core.KettleVariables;
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

	public KettleRemoteClient(KettleRepositoryClient repositoryClient, final SlaveServer remoteServer)
			throws KettleException {
		this.repositoryClient = repositoryClient;
		this.remoteServer = remoteServer;
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

	/**
	 * 远端记录进程
	 * 
	 * @author Administrator
	 *
	 */
	// private class RemoteRecordDaemon implements Runnable {
	//
	// private final List<KettleRecord> updateRecords = new
	// LinkedList<KettleRecord>();
	//
	// @Override
	// public void run() {
	// logger.debug("Kettle远端[" + getHostName() + "]定时任务轮询启动!");
	// updateRecords.clear();
	// if (checkRemoteStatus()) {
	// // 将Null排到最后
	// Arrays.sort(recordArr, new Comparator<KettleRecord>() {
	// public int compare(KettleRecord o1, KettleRecord o2) {
	// if (o1 == null) {
	// return 1;
	// }
	// if (o2 == null) {
	// return -1;
	// }
	// return 0;
	// }
	// });
	// for (int i = 0; i < recordArr.length; i++) {
	// if (recordArr[i] != null) {
	// dealRunningRecord(recordArr[i]);
	// if (recordArr[i].isError() || recordArr[i].isFinished()) {
	// updateRecords.add(recordArr[i]);
	// recordArr[i] = null;
	// }
	// }
	// // 尝试获取
	// if (recordArr[i] == null) {
	// recordArr[i] = kettleRemotePool.getKettleRecordPool().nextRecord();
	// }
	// // 申请状态
	// if (recordArr[i] != null) {
	// dealNotSendRecord(recordArr[i]);
	// } else {
	// // 如果没有任务,则直接下一步
	// break;
	// }
	// }
	// } else {
	// for (int i = 0; i < recordArr.length; i++) {
	// if (recordArr[i] != null) {
	// dealErrorRemoteRecord(recordArr[i]);
	// if (recordArr[i].isError() || recordArr[i].isFinished()) {
	// updateRecords.add(recordArr[i]);
	// recordArr[i] = null;
	// }
	// }
	// }
	// recoveryStatus();
	// }
	// if (!updateRecords.isEmpty()) {
	// cleanRecords();
	// }
	// logger.debug("Kettle远端[" + getHostName() + "]定时任务轮询完成!");
	// }
	//
	// /**
	// * @param 清理
	// */
	// private void cleanRecords() {
	// for (KettleRecord roll : updateRecords) {
	// // 完成的进行清理
	// if (roll.isFinished()) {
	// remoteRemoveJobNE(roll.getName(), roll.getRunID());
	// }
	// }
	// }
	//
	// /**
	// * 在远端无法连接时,所有运行中的任务为异常,Apply任务进入Record池
	// */
	// private void dealErrorRemoteRecord(KettleRecord record) {
	// if (record.isApply()) {
	// kettleRemotePool.getKettleRecordPool().addPrioritizeRecord(record);
	// } else if (record.isRunning()) {
	// record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
	// }
	// }
	//
	// /**
	// * 处理运行中的任务
	// *
	// * @param roll
	// */
	// private void dealRunningRecord(KettleRecord roll) {
	// if (roll.isRunning()) {
	// String status = null;
	// KettleRecord job = (KettleRecord) roll;
	// try {
	// status = remoteJobStatus(job);
	// } catch (Exception e) {
	// logger.error("Kettle远端[" + getHostName() + "]查询Record[" + job.getUuid() +
	// "]发生异常\n", e);
	// status = KettleVariables.RECORD_STATUS_ERROR;
	// }
	// job.setStatus(status);
	// checkJobRunOvertime(job);
	// if (roll.isError() || roll.isFinished()) {
	// try {
	// kettleRemotePool.getDbClient().updateRecord(roll);
	// } catch (KettleException e) {
	// logger.error("Kettle更新Record[" + job.getUuid() + "]状态[" +
	// roll.getStatus() + "]发生异常\n", e);
	// }
	// }
	// }
	// }
	//
	// /**
	// * 是否超时
	// *
	// * @param job
	// */
	// private void checkJobRunOvertime(KettleRecord job) {
	// if (job.isRunning() && recordRunningMax != null && recordRunningMax > 0)
	// {
	// if ((System.currentTimeMillis() - job.getUpdateTime().getTime()) / 1000 /
	// 60 > recordRunningMax) {
	// remoteStopJobNE(job);
	// job.setStatus(KettleVariables.RECORD_STATUS_ERROR);
	// job.setErrMsg("Record[" + job.getUuid() + "]执行超时,异常状态!");
	// }
	// }
	// }
	//
	// /**
	// * 处理未远程推送的任务
	// *
	// * @param roll
	// */
	// private void dealNotSendRecord(KettleRecord job) {
	// if (job.isApply() || job.isRepeat()) {
	// try {
	// if (job.getKettleMeta() == null) {
	// job.setKettleMeta(kettleRemotePool.getRepositoryClient().getJobMeta(job.getJobid()));
	// }
	// String runID = remoteSendJob(job.getKettleMeta());
	// job.setRunID(runID);
	// job.setHostname(getHostName());
	// job.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
	// updateRecords.add(job);
	// } catch (Exception e) {
	// logger.error("Kettle远端[" + getHostName() + "]发送Record[" + job.getUuid() +
	// "]发生异常\n", e);
	// job.setStatus(KettleVariables.REMOTE_STATUS_ERROR);
	// job.setErrMsg("Kettle远端[" + getHostName() + "]发送Record[" + job.getUuid()
	// + "]发生异常");
	// job.setHostname(getHostName());
	// updateRecords.add(job);
	// }
	// }
	// }
	// }
}
