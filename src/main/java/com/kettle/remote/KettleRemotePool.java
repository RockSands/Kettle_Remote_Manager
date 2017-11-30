package com.kettle.remote;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.instance.KettleMgrInstance;

/**
 * Kettle远程池,仅维护远端的状态
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {
	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRemotePool.class);

	/**
	 * 远程连接
	 */
	private final Map<String, KettleRemoteClient> remoteclients;

	/**
	 * 设备名称
	 */
	private final List<String> hostNames = new LinkedList<String>();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();

	/**
	 * 池状态
	 */
	private String poolStatus = KettleVariables.RECORD_STATUS_RUNNING;

	/**
	 * @param repositoryClient
	 * @throws Exception
	 */
	public KettleRemotePool() throws Exception {
		this.remoteclients = new HashMap<String, KettleRemoteClient>();
		for (SlaveServer server : KettleMgrInstance.kettleMgrEnvironment.getRepositoryClient().getRepository()
				.getSlaveServers()) {
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			addRemoteClient(
					new KettleRemoteClient(KettleMgrInstance.kettleMgrEnvironment.getRepositoryClient(), server));
			hostNames.add(server.getHostname());
		}
		logger.info("Kettle远程池已经加载Client" + remoteclients.keySet());
		// 每30秒同步一次状态
		threadPool.scheduleAtFixedRate(new RemoteDeamon(), 10, 30, TimeUnit.SECONDS);
	}

	/**
	 * 添加Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void addRemoteClient(KettleRemoteClient remoteClient) {
		if (remoteclients.containsKey(remoteClient.getHostName())) {
			logger.error("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]失败,该主机已存在!");
		} else {
			remoteclients.put(remoteClient.getHostName(), remoteClient);
			logger.info("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]成功!");
		}
	}

	/**
	 * 获取所有Client
	 * 
	 * @return
	 */
	public Collection<KettleRemoteClient> getRemoteclients() {
		return remoteclients.values();
	}

	/**
	 * 远程池
	 * 
	 * @author chenkw
	 *
	 */
	private class RemoteDeamon implements Runnable {

		@Override
		public void run() {
			Collection<KettleRemoteClient> clients = remoteclients.values();
			for (KettleRemoteClient client : clients) {
				client.refreshRemoteStatus();
			}
			String status = KettleVariables.REMOTE_STATUS_ERROR;
			for (KettleRemoteClient client : clients) {
				if (client.isRunning()) {
					status = KettleVariables.REMOTE_STATUS_RUNNING;
				} else {
					logger.error("Remote[" + client.getHostName() + "]异常状态!");
				}
			}
			poolStatus = status;
		}
	}

	/**
	 * 查看状态
	 * 
	 * @return
	 */
	public String getPoolStatus() {
		return poolStatus;
	}
}
