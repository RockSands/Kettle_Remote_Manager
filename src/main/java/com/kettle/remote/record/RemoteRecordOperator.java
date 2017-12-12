package com.kettle.remote.record;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordRelation;
import com.kettle.record.operation.BaseRecordOperator;
import com.kettle.remote.KettleRemoteClient;

public class RemoteRecordOperator extends BaseRecordOperator {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteRecordOperator.class);

	/**
	 * 远端
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * Kettle资源库
	 */
	private final KettleRepositoryClient repositoryClient;

	/**
	 * @param remoteClient
	 */
	public RemoteRecordOperator(KettleRemoteClient remoteClient) {
		this.remoteClient = remoteClient;
		this.repositoryClient = KettleMgrInstance.kettleMgrEnvironment.getRepositoryClient();
	}

	@Override
	public boolean attachRecord(KettleRecord record) {
		if (remoteClient.isRunning() && super.attachRecord(record)) {
			return true;
		}
		return false;
	}

	/**
	 * 强制加载,无视远端状态
	 * 
	 * @param record
	 * @return
	 */
	public boolean attachRecordForce(KettleRecord record) {
		return super.attachRecord(record);
	}

	/**
	 * @throws KettleException
	 */
	@Override
	public void dealRecord() throws KettleException {
		if (isAttached()) {
			if (remoteClient.isRunning()) {
				super.dealRecord();
			} else if (!record.isApply()) {
				dealErrorRemoteRecord();
			}
		}
	}

	/**
	 * @throws KettleException
	 */
	private void updateRecord() throws KettleException {
		try {
			dbClient.updateRecord(record);
		} catch (Exception ex) {
			throw new KettleException("remote[" + remoteClient.getHostName() + "]持久化更新Job[" + record.getUuid() + "]失败!",
					ex);
		}
	}

	@Override
	public void dealApply() throws KettleException {
		String runID = null;
		try {
			runID = remoteClient.remoteSendJob(record);
			record.setRunID(runID);
			record.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		} catch (Exception ex) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg("remote[" + remoteClient.getHostName() + "]发送Job[" + record.getUuid() + "]发生异常");
			logger.error("remote[" + remoteClient.getHostName() + "]发送Job[" + record.getUuid() + "]发生异常!", ex);
		}
		record.setHostname(remoteClient.getHostName());
		updateRecord();
	}

	@Override
	public void dealRegiste() throws KettleException {
		throw new KettleException("Record[" + record.getUuid() + "] 状态为[Registe],无法远程执行!");
	}

	@Override
	public void dealError() throws KettleException {
		try {
			dbClient.updateRecord(record);
		} catch (Exception e) {
			throw new KettleException("Record[" + record.getUuid() + "] 状态为[Error],数据库发生异常!", e);
		}
	}

	@Override
	public void dealFinished() throws KettleException {
		try {
			dbClient.updateRecord(record);
			remoteClient.remoteRemoveJobNE(record);
		} catch (Exception e) {
			throw new KettleException("Record[" + record.getUuid() + "] 状态为[Finished],数据库发生异常!", e);
		}
	}

	@Override
	public void dealRunning() throws KettleException {
		try {
			remoteClient.remoteJobStatus(record);
			checkJobRunOvertime();
		} catch (Exception e) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg("Record[" + record.getUuid() + "] 在Remote[" + remoteClient.getHostName() + "]中同步状态发生异常!");
			logger.error("Record[" + record.getUuid() + "] 在Remote[" + remoteClient.getHostName() + "]中同步状态发生异常!", e);
		} finally {
			if (!record.isRunning()) {
				// 重新处理
				super.dealRecord();
			}
		}
	}

	@Override
	public void dealRemoving() {
		if (record.isRunning()) {// 如果远程是运行状态
			remoteClient.remoteStopJobNE(record);
			remoteClient.remoteRemoveJobNE(record);
		}
		record.setStatus(KettleVariables.RECORD_STATUS_REMOVING);
		try {
			dbClient.deleteRecord(record.getUuid());
			List<KettleRecordRelation> relations = dbClient.deleteDependentsRelationNE(record.getUuid());
			repositoryClient.deleteJobEntireDefineNE(relations);
		} catch (Exception e) {
			logger.error("Record[" + record.getUuid() + "]状态Removing,后台删除失败！", e);
		}
		detachRecord();
	}

	/**
	 * 处理远端无法连接的记录
	 * 
	 * @throws KettleException
	 */
	private void dealErrorRemoteRecord() throws KettleException {
		record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
		record.setErrMsg("Remote[" + remoteClient.getHostName() + "]状态异常,Record[" + record.getUuid() + "]");
		updateRecord();
	}

	/**
	 * 是否超时
	 * 
	 */
	private void checkJobRunOvertime() {
		if (record.isRunning() && KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT != null
				&& KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT > 0) {
			if ((System.currentTimeMillis() - record.getUpdateTime().getTime()) / 1000
					/ 60 > KettleMgrEnvironment.KETTLE_RECORD_RUNNING_TIMEOUT) {
				remoteClient.remoteStopJobNE(record);
				record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
				record.setErrMsg("Record[" + record.getUuid() + "]执行超时,异常状态!");
			}
		}
	}

	/**
	 * 返回Client
	 * 
	 * @return
	 */
	public KettleRemoteClient getRemoteClient() {
		return remoteClient;
	}
}
