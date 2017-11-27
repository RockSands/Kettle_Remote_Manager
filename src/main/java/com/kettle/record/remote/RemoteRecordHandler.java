package com.kettle.record.remote;

import java.util.Arrays;
import java.util.Comparator;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleRecord;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

public class RemoteRecordHandler implements Runnable {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteRecordHandler.class);
	/**
	 * 远端
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * 数据库
	 */
	private final KettleDBClient dbClient;

	/**
	 * 处理的任务
	 */
	private final KettleRecord[] recordArr;

	/**
	 * @param client
	 */
	public RemoteRecordHandler(KettleRemoteClient remoteClient) {
		this.remoteClient = remoteClient;
		this.recordArr = new KettleRecord[remoteClient.maxRecord];
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	}

	/**
	 * 排序,将Null放到最后
	 */
	private void sortRecords() {
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
	}

	/**
	 * 是否超时
	 * 
	 * @param job
	 */
	private void checkJobRunOvertime(KettleRecord job) {
		if (job.isRunning() && KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT != null
				&& KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT > 0) {
			if ((System.currentTimeMillis() - job.getUpdateTime().getTime()) / 1000
					/ 60 > KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT) {
				remoteClient.remoteStopJobNE(job);
				job.setStatus(KettleVariables.RECORD_STATUS_ERROR);
				job.setErrMsg("Record[" + job.getUuid() + "]执行超时,异常状态!");
			}
		}
	}

	/**
	 * 处理运行中的任务
	 * 
	 * @param roll
	 */
	private void dealHasSendRecord(int index) {
		if (recordArr[index].isRunning()) {
			try {
				remoteClient.remoteJobStatus(recordArr[index]);
			} catch (Exception e) {
				logger.error("Kettle远端[" + remoteClient.getHostName() + "]查询Record[" + recordArr[index].getUuid()
						+ "]发生异常\n", e);
				recordArr[index].setStatus(KettleVariables.RECORD_STATUS_ERROR);
				recordArr[index].setErrMsg(e.getMessage());
			}
			checkJobRunOvertime(recordArr[index]);
		}
	}

	/**
	 * 更新记录
	 * 
	 * @param i
	 */
	private void updateRecord(int index) {
		try {
			if (recordArr[index].isError() || recordArr[index].isFinished()) {
				dbClient.updateRecord(recordArr[index]);
			}
		} catch (Exception e) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]更新任务[" + recordArr[index].getJobid() + "]失败!", e);
		}
	}

	/**
	 * @param 清理任务
	 * @throws KettleException
	 */
	private void cleanRecord(int index) {
		if (recordArr[index].isFinished()) {
			remoteClient.remoteRemoveJobNE(recordArr[index]);
		}
		if (recordArr[index].isError() || recordArr[index].isFinished()) {
			recordPool.deleteRecord(recordArr[index].getUuid());
			recordArr[index] = null;
		}
	}

	/**
	 * 处理未远程推送的任务
	 * 
	 * @param roll
	 */
	private void dealNotSendRecord(int index) {
		if (recordArr[index].isApply() || recordArr[index].isRepeat()) {
			try {
				String runID = remoteClient.remoteSendJob(recordArr[index]);
				recordArr[index].setRunID(runID);
				recordArr[index].setHostname(remoteClient.getHostName());
				recordArr[index].setStatus(KettleVariables.RECORD_STATUS_RUNNING);
			} catch (Exception e) {
				logger.error("Kettle远端[" + remoteClient.getHostName() + "]发送Record[" + recordArr[index].getUuid()
						+ "]发生异常\n", e);
				recordArr[index].setStatus(KettleVariables.REMOTE_STATUS_ERROR);
				recordArr[index].setErrMsg(
						"Kettle远端[" + remoteClient.getHostName() + "]发送Record[" + recordArr[index].getUuid() + "]发生异常");
				recordArr[index].setHostname(remoteClient.getHostName());
			}
		}
	}

	/**
	 * 在远端无法连接时,所有运行中的任务为异常,Apply任务进入Record池
	 */
	private void dealErrorRemoteRecord(int index) {
		if (recordArr[index].isApply()) {
			recordPool.addPrioritizeRecord(recordArr[index]);
		} else if (recordArr[index].isRunning()) {
			recordArr[index].setStatus(KettleVariables.RECORD_STATUS_ERROR);
		}
	}

	@Override
	public void run() {
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询启动!");
		try {
			sortRecords();
			if (remoteClient.isRunning()) {
				for (int index = 0; index < recordArr.length; index++) {
					if (recordArr[index] != null) {
						dealHasSendRecord(index);
						updateRecord(index);
						cleanRecord(index);
					}
					// 尝试获取
					if (recordArr[index] == null) {
						recordArr[index] = recordPool.nextRecord();
					}
					// 申请状态
					if (recordArr[index] != null) {
						dealNotSendRecord(index);
						updateRecord(index);
					} else {
						// 如果没有任务,则直接下一步
						break;
					}
				}
			} else {
				for (int index = 0; index < recordArr.length; index++) {
					if (recordArr[index] != null) {
						dealErrorRemoteRecord(index);
						updateRecord(index);
						cleanRecord(index);
					}
				}
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]定时任务发生异常成!", ex);
		}
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询完成!");
	}
}
