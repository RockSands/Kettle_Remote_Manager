package com.kettle.remote.record.distribute;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleTransRecord;
import com.kettle.remote.KettleRemoteClient;

public class RemoteRecordDaemon implements Runnable {

	/**
	 * 远端连接
	 */
	private final KettleRemoteClient remote;

	/**
	 * 记录
	 */
	private KettleRecord record;

	public RemoteRecordDaemon(KettleRemoteClient remote) {
		this.remote = remote;
	}

	/**
	 * 获取
	 * 
	 * @return
	 */
	public KettleRecord getRecord() {
		return record;
	}

	/**
	 * 设置
	 * 
	 * @param record
	 */
	public void setRecord(KettleRecord record) {
		this.record = record;
	}

	@Override
	public void run() {
		if (record != null && record.isRunning()) {
			String status = null;
			try {
				if (KettleTransRecord.class.isInstance(record)) {
					status = remote.remoteTransStatus(record.getName());
				} else if (KettleJobRecord.class.isInstance(record)) {
					status = remote.remoteJobStatus(record.getName());
				} else {
					status = KettleVariables.RECORD_STATUS_ERROR;
				}
				record.setStatus(status);
			} catch (Exception ex) {
				status = KettleVariables.RECORD_STATUS_ERROR;
				record.setStatus(status);
			}
			remote.recordHasSync(record);
		}
	}

	/**
	 * 是否空闲
	 * 
	 * @return
	 */
	public boolean isFree() {
		return record == null;
	}

	public KettleRemoteClient getRemote() {
		return remote;
	}
}
