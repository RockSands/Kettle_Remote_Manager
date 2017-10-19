package com.kettle.remote;

import com.kettle.bean.KettleJobRecord;
import com.kettle.bean.KettleRecord;
import com.kettle.bean.KettleTransRecord;
import com.kettle.core.KettleVariables;

class RemoteRecordDaemon implements Runnable {
	/**
	 * 远端连接
	 */
	private final KettleRemoteClient remote;

	private final KettleRecord record;

	public RemoteRecordDaemon(final KettleRemoteClient remote, final KettleRecord record) {
		this.remote = remote;
		this.record = record;
	}

	@Override
	public void run() {
		String status = null;
		try {
			if (KettleTransRecord.class.isInstance(record)) {
				status = remote.remoteTransStatus(record.getName());
			} else if (KettleJobRecord.class.isInstance(record)) {
				status = remote.remoteJobStatus(record.getName());
			} else {
				status = KettleVariables.RECORD_STATUS_ERROR;
			}

		} catch (Exception ex) {
			status = KettleVariables.RECORD_STATUS_ERROR;
			remote.checkRemoteStatus();
		}
		record.setStatus(status);
	}
}
