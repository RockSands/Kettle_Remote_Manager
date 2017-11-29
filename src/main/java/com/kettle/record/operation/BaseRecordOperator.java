package com.kettle.record.operation;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleRecord;

public abstract class BaseRecordOperator implements IRecordOperator {

	/**
	 * 任务
	 */
	protected KettleRecord record;

	@Override
	public KettleRecord getRecord() {
		return record;
	}

	public String getRecordStatus() {
		return record.getStatus();
	}

	@Override
	public synchronized boolean attachRecord(KettleRecord record) {
		if (record == null){
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
		return record;
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
	 * 是否受理状态
	 * 
	 * @return
	 */
	public boolean isApply() {
		if (!isAttached()) {
			return false;
		}
		return KettleVariables.RECORD_STATUS_APPLY.equals(record.getStatus());
	}

	/**
	 * 是否异常状态
	 * 
	 * @return
	 */
	public boolean isError() {
		if (!isAttached()) {
			return false;
		}
		return KettleVariables.RECORD_STATUS_ERROR.equals(record.getStatus());
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
		return KettleVariables.RECORD_STATUS_FINISHED.equals(record.getStatus());
	}

	/**
	 * 处理任务
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void dealRecord() throws KettleException {
		if (record.isApply()) {
			dealApply();
		} else if (record.isRegiste()) {
			dealRegiste();
		} else if (record.isRepeat()) {
			dealRepeat();
		} else if (record.isRunning()) {
			dealRunning();
		} else if (record.isFinished()) {
			dealFinished();
		} else {
			dealError();
		}
	}
}
