package com.kettle.record.remote;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.pentaho.di.core.util.EnvUtil;

import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemotePool;

public class RemoteRecordProcess {
	/**
	 * 远程池
	 */
	private final List<RemoteRecordHandler> handlers;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * 定时任务
	 */
	private final ScheduledExecutorService threadPool;
	
	public RemoteRecordProcess(KettleRecordPool recordPool, KettleRemotePool remotePool) {
		int remoteRecordRatio  = EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_ID"),
	}

}
