package com.kettle.record.remote;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.KettleRemotePool;

public class RemoteRecordProcess {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteRecordProcess.class);

	/**
	 * 远程池
	 */
	private List<RemoteRecordHandler> handlers;

	/**
	 * 定时任务
	 */
	private ScheduledExecutorService threadPool;

	/**
	 * @param recordPool
	 * @param remotePool
	 */
	public RemoteRecordProcess() {
		this.handlers = new LinkedList<RemoteRecordHandler>();
		KettleRemotePool remotePool = KettleMgrInstance.kettleMgrEnvironment.getRemotePool();
		threadPool = Executors.newScheduledThreadPool(remotePool.getRemoteclients().size());
		for (KettleRemoteClient remoteClient : remotePool.getRemoteclients()) {
			handlers.add(new RemoteRecordHandler(remoteClient));
		}
	}

	/**
	 * 启动
	 * 
	 * @param remotePool
	 */
	public void start() {
		for (int index = 0; index < KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE; index++) {
			threadPool.scheduleAtFixedRate(handlers.get(index), 2 * index, 20, TimeUnit.SECONDS);
		}
		logger.info("Kettle远程任务关系系统的线程启动完成,个数:" + handlers.size());
	}
}
