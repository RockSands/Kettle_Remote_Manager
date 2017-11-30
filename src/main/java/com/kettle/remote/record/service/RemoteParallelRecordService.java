package com.kettle.remote.record.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.instance.KettleMgrInstance;
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
	 * 定时任务
	 */
	protected final ScheduledExecutorService threadPool;

	/**
	 * 处理单元标记
	 */
	private int handlerIndex = 0;

	/**
	 * 构造器
	 */
	public RemoteParallelRecordService() {
		KettleRemotePool remotePool = KettleMgrInstance.kettleMgrEnvironment.getRemotePool();
		for (KettleRemoteClient remoteClient : remotePool.getRemoteclients()) {
			for (int i = 0, size = remoteClient.maxRecord; i < size; i++) {
				handlers.add(new RemoteParallelRecordHandler(i, remoteClient));
			}
		}
		threadPool = Executors.newScheduledThreadPool(handlers.size());
		KettleMgrInstance.kettleMgrEnvironment.getRecordPool().registePoolMonitor(this);
		super.attachOldRecord();
	}

	@Override
	public synchronized void addRecordNotify() {
		logger.debug("RecordPool增加一条记录,尝试启动空闲处理单元!");
		RemoteParallelRecordHandler handler = null;
		for (int i = 0, size = handlers.size(); i < size; i++, handlerIndex++) {
			if (handlerIndex >= size) {
				handlerIndex = 0;
			}
			handler = handlers.get(handlerIndex);
			if (handler.isRunning()) {
				threadPool.scheduleAtFixedRate(handlers.get(handlerIndex), 0, 10, TimeUnit.SECONDS);
				break;
			}
		}
	}
}
