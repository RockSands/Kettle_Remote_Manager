package com.kettle.remote.record;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecordPool;
import com.kettle.record.operation.BaseRecordOperator;
import com.kettle.remote.KettleRemoteClient;

/**
 * 远程处理任务
 * 
 * @author Administrator
 *
 */
public class RemoteRecordOperator extends BaseRecordOperator {

	private static Logger logger = LoggerFactory.getLogger(RemoteRecordOperator.class);

	/**
	 * 远程连接
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * 数据库
	 */
	private final KettleDBClient dbClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * @param remoteClient
	 */
	public RemoteRecordOperator(KettleRemoteClient remoteClient) {
		this.remoteClient = remoteClient;
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	}

	/**
	 * 处理任务
	 * 
	 * @param record
	 * @throws KettleException
	 */
	@Override
	public void dealRecord() throws KettleException {
		if (remoteClient.isRunning()) {
			super.dealRecord();
		} else {
			dealErrorRemoteRecord();
		}
	}

	@Override
	public void dealApply() throws KettleException {
		remoteClient.remoteSendJob(record);
	}

	@Override
	public void dealRepeat() throws KettleException {
		dealApply();
	}

	@Override
	public void dealRegiste() throws KettleException {
		throw new KettleException("Record[" + record.getUuid() + "] 状态为[Registed],无法远程执行!");
	}

	@Override
	public void dealError() throws KettleException {
		try {
			dbClient.updateRecord(record);
			cleanRecord();
		} catch (Exception e) {
			throw new KettleException("Record[" + record.getUuid() + "] 状态为[Error],数据库发生异常!", e);
		}
	}

	@Override
	public void dealFinished() throws KettleException {
		try {
			dbClient.updateRecord(record);
			cleanRecord();
		} catch (Exception e) {
			throw new KettleException("Record[" + record.getUuid() + "] 状态为[Finished],数据库发生异常!", e);
		}
	}

	@Override
	public void dealRunning() throws KettleException {
		try {
			remoteClient.remoteJobStatus(record);
			checkJobRunOvertime();
			if (!record.isRunning()) {
				// 重新处理
				super.dealRecord();
			}
		} catch (Exception e) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg("Record[" + record.getUuid() + "] 在Remote[" + remoteClient.getHostName() + "]中同步状态发生异常!");
			logger.error("Record[" + record.getUuid() + "] 在Remote[" + remoteClient.getHostName() + "]中同步状态发生异常!", e);
		}
	}

	/**
	 * 在远端无法连接时,所有运行中的任务为异常,Apply任务进入Record池
	 */
	private void dealErrorRemoteRecord() {
		if (record.isApply()) {
			recordPool.addPrioritizeRecord(record);
			super.setRecord(null);
		} else if (record.isRunning()) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg("Remote[" + remoteClient.getHostName() + "]状态异常,Record[" + record.getUuid() + "]");
		}
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
	 * @param 清理任务
	 */
	private void cleanRecord() {
		if (record.isFinished()) {
			remoteClient.remoteRemoveJobNE(record);
		}
		if (record.isError() || record.isFinished()) {
			recordPool.deleteRecord(record.getUuid());
		}
	}
}
