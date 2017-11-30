package com.kettle.remote.record.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.kettle.record.KettleRecord;
import com.kettle.record.service.RecordService;
import com.kettle.remote.KettleRemoteClient;
import com.kettle.remote.record.RemoteParallelRecordHandler;

/**
 * Kettle远程并行服务,模型比较主动
 * 
 * @author Administrator
 *
 */
public class RemoteParallelRecordService extends RecordService {
	/**
	 * 远程执行则
	 */
	private final List<RemoteParallelRecordHandler> handlers = new ArrayList<RemoteParallelRecordHandler>();

	/**
	 * 构造器
	 */
	public RemoteParallelRecordService() {
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
			for (int i = 0, size = remoteClient.maxRecord; i < size; i++) {
				handlers.add(
						new RemoteParallelRecordHandler(remoteClient, oldRecordMap.get(remoteClient.getHostName())));
			}
		}
		for (KettleRecord record : oldRecords) {
			super.recordPool.addPrioritizeRecord(record);
		}
	}
}
