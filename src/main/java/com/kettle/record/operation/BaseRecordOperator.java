package com.kettle.record.operation;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.KettleVariables;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;

public abstract class BaseRecordOperator implements IRecordOperator {

	/**
	 * 任务
	 */
	protected KettleRecord record;

	/**
	 * 数据库
	 */
	protected final KettleDBClient dbClient;

	public BaseRecordOperator() {
		this.dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	}

	@Override
	public KettleRecord getRecord() {
		return record;
	}

	public String getRecordStatus() {
		return record.getStatus();
	}

	@Override
	public synchronized boolean attachRecord(KettleRecord record) {
		if (record == null) {
			return false;
		}
		if (this.record != null) {
			return false;
		}
		this.record = record;
		return true;
	}

	@Override
	public KettleRecord detachRecord() {
		KettleRecord kettleRecord = record;
		record = null;
		return kettleRecord;
	}

	@Override
	public boolean isAttached() {
		return record != null;
	}

	/**
	 * 是否运行状态
	 * 
	 * @return
	 */
	public boolean isRunning() {
		if (!isAttached()) {
			return false;
		}
		return KettleVariables.RECORD_STATUS_RUNNING.equals(record.getStatus());
	}

	/**
	 * 是否完成中
	 * 
	 * @return
	 */
	public boolean isFinished() {
		if (!isAttached()) {
			return false;
		}
		return KettleVariables.RECORD_STATUS_FINISHED.equals(record.getStatus())
				|| KettleVariables.RECORD_STATUS_ERROR.equals(record.getStatus());
	}

	/**
	 * 处理任务
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void dealRecord() throws KettleException {
		KettleRecord recordTMP = dbClient.queryRecord(record.getUuid());
		if (recordTMP == null || recordTMP.isRemoving()) {
			dealRemoving();
		}
		if (record.isApply()) {
			dealApply();
		} else if (record.isRegiste()) {
			dealRegiste();
		} else if (record.isRunning()) {
			dealRunning();
		} else if (record.isFinished()) {
			dealFinished();
		} else {
			dealError();
		}
	}
}
