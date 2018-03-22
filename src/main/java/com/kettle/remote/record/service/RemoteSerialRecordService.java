package com.kettle.remote.record.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;
import com.kettle.record.service.RecordService;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.KettleRemotePool;
import com.kettle.remote.record.RemoteSerialRecordHandler;

/**
 * @author Administrator
 *
 */
public class RemoteSerialRecordService extends RecordService {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteSerialRecordService.class);

	/**
	 * 远程池
	 */
	private final List<RemoteSerialRecordHandler> handlers = new LinkedList<RemoteSerialRecordHandler>();;

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool;

	/**
	 * 运行中
	 */
	private Boolean hasStart;

	/**
	 * 构造器
	 */
	public RemoteSerialRecordService() {
		super();
		KettleRemotePool remotePool = KettleMgrInstance.kettleMgrEnvironment.getRemotePool();
		threadPool = Executors.newScheduledThreadPool(remotePool.getRemoteclients().size());
		List<KettleRecord> oldRecords = super.getHandleRecords();
		Map<String, List<KettleRecord>> oldRecordMap = new HashMap<String, List<KettleRecord>>();
		for (KettleRecord record : oldRecords) {
			if (record == null) {
				continue;
			}
			if (record.isApply()) {
				super.recordPool.addPrioritizeRecord(record);
			} else if (record.getHostname() != null) {
				if (!oldRecordMap.containsKey(record.getHostname())) {
					oldRecordMap.put(record.getHostname(), new LinkedList<KettleRecord>());
				}
				oldRecordMap.get(record.getHostname()).add(record);
			}
		}
		RemoteSerialRecordHandler recordHandler = null;
		for (KettleRemoteClient remoteClient : remotePool.getRemoteclients()) {
			recordHandler = new RemoteSerialRecordHandler(remoteClient, oldRecordMap.get(remoteClient.getHostName()));
			handlers.add(recordHandler);
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
				threadPool.scheduleAtFixedRate(handlers.get(index), 2 * index, 10, TimeUnit.SECONDS);
			}
			logger.info("Kettle远程任务关系系统的线程启动完成,个数:" + handlers.size());
		} else {
			logger.info("Kettle远程任务关系系统的线程已经启动,无法再次启动!");
		}
	}

	@Override
	protected void jobMustDie(KettleRecord record) throws KettleException {
		for (RemoteSerialRecordHandler handler : handlers) {
			if (handler.tryRemoveRecord(record)) {
				break;
			}
		}
		// 直接清除
		dbClient.deleteRecord(record.getUuid());
	}
}
