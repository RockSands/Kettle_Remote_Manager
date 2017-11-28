package com.kettle.remote.record.process;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleRecord;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecordPool;
import com.kettle.record.KettleRecordRelation;
import com.kettle.record.service.RecordService;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.KettleRemotePool;
import com.kettle.remote.record.RemoteSerialRecordHandler;
import com.kettle.remote.record.RemoteParallelRecordHandler;

public class RemoteParallelRecordService extends RecordService {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteParallelRecordService.class);

	/**
	 * 远程执行则
	 */
	private List<RemoteParallelRecordHandler> operators;

	/**
	 * 定时任务
	 */
	private ScheduledExecutorService threadPool;

	/**
	 * 运行中
	 */
	private Boolean hasStart;

	/**
	 * 
	 */
	public RemoteParallelRecordService() {
		KettleRemotePool remotePool = KettleMgrInstance.kettleMgrEnvironment.getRemotePool();
		threadPool = Executors.newScheduledThreadPool(remotePool.getRemoteclients().size() * 10);
		for (KettleRemoteClient remoteClient : remotePool.getRemoteclients()) {
			for (int i = 0; i < 10; i++) {
				operators.add(new RemoteParallelRecordHandler(remoteClient));
			}
		}
		start();
	}

	/**
	 * 启动
	 * 
	 * @param remotePool
	 */
	private void start() {
		if (hasStart == null || !hasStart.booleanValue()) {
			for (int index = 0; index < handlers.size(); index++) {
				threadPool.scheduleAtFixedRate(handlers.get(index), 2 * index, 20, TimeUnit.SECONDS);
			}
			logger.info("Kettle远程任务关系系统的线程启动完成,个数:" + handlers.size());
		} else {
			logger.info("Kettle远程任务关系系统的线程已经启动,无法再次启动!");
		}
	}
}
