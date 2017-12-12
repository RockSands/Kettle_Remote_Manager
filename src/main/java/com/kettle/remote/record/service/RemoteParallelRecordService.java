package com.kettle.remote.record.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;
import com.kettle.record.pool.KettleRecordPoolMonitor;
import com.kettle.record.service.RecordService;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.KettleRemotePool;
import com.kettle.remote.record.RemoteParallelRecordHandler;

/**
 * Kettle远程并行服务,模型比较主动
 * 
 * @author Administrator
 *
 */
public class RemoteParallelRecordService extends RecordService implements KettleRecordPoolMonitor {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteParallelRecordService.class);
	/**
	 * 远程执行则
	 */
	private final List<RemoteParallelRecordHandler> handlers = new ArrayList<RemoteParallelRecordHandler>();

	/**
	 * 线程池
	 */
	private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

	/**
	 * 构造器
	 */
	public RemoteParallelRecordService() {
		KettleRemotePool remotePool = KettleMgrInstance.kettleMgrEnvironment.getRemotePool();
		List<KettleRecord> oldRecords = super.getOldRecords();
		Map<String, List<KettleRecord>> oldRecordMap = new HashMap<String, List<KettleRecord>>();
		KettleRecord recordIndex = null;
		// 分类,并将apply状态的留在oldRecords中
		for (Iterator<KettleRecord> it = oldRecords.iterator(); it.hasNext();) {
			recordIndex = it.next();
			if (recordIndex == null) {
				it.remove();
				continue;
			}
			if (recordIndex.isRunning() && recordIndex.getHostname() != null) {
				if (!oldRecordMap.containsKey(recordIndex.getHostname())) {
					oldRecordMap.put(recordIndex.getHostname(), new LinkedList<KettleRecord>());
				}
				oldRecordMap.get(recordIndex.getHostname()).add(recordIndex);
				it.remove();
			}
		}
		for (KettleRemoteClient remoteClient : remotePool.getRemoteclients()) {
			handlers.add(new RemoteParallelRecordHandler(remoteClient, oldRecordMap.get(remoteClient.getHostName())));
		}
		for (KettleRecord record : oldRecords) {
			super.recordPool.addPrioritizeRecord(record);
		}
		// 注册监听
		recordPool.registePoolMonitor(this);
		// 申请
		addRecordNotify();
	}

	@Override
	public void addRecordNotify() {
		for (final RemoteParallelRecordHandler handler : handlers) {
			// 尝试唤醒
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						handler.tryAwaken();
					} catch (Exception ex) {
						logger.error("RemoteParallelRecordService发生异常!\n", ex);
					}
				}
			});
		}
	}
}
