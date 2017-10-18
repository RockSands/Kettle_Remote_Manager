package com.kettle.remote;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.repo.KettleDBRepositoryClient;

/**
 * 保存远程运行池
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {

	Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	private final Map<String, KettleRemoteClient> remoteclients;

	/**
	 * 线程池
	 */
	private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	/**
	 * @param dbRepositoryClient
	 * @param includeServers
	 * @param excludeServers
	 * @throws KettleException
	 */
	public KettleRemotePool(final KettleDBRepositoryClient dbRepositoryClient, List<String> includeHostNames,
			List<String> excludeHostNames) throws KettleException {
		remoteclients = new HashMap<String, KettleRemoteClient>();
		KettleRemoteClient remoteClient = null;
		for (SlaveServer server : dbRepositoryClient.getRepository().getSlaveServers()) {
			if (excludeHostNames != null && excludeHostNames.contains(server.getHostname())) {
				continue;
			}
			if (includeHostNames != null && !includeHostNames.contains(server.getHostname())) {
				continue;
			}
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			remoteClient = new KettleRemoteClient(dbRepositoryClient, server);
			if (remoteclients.containsKey(remoteClient.getHostName())) {
				throw new KettleException("远程池启动失败,存在Hostname重复的主机!");
			}
			remoteclients.put(remoteClient.getHostName(), remoteClient);
		}
		if (remoteclients.isEmpty()) {
			throw new RuntimeException("KettleRemotePool初始化失败,未找到可用的远端服务!");
		}
	}

	/**
	 * 添加Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void addRemoteClient(KettleRemoteClient remoteClient) {
		synchronized (remoteclients) {
			if (remoteClient.checkStatus() && remoteclients.containsKey(remoteClient.getHostName())) {
				logger.info("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]");
				remoteclients.put(remoteClient.getHostName(), remoteClient);
			}
		}
	}

	/**
	 * 处理Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void dealErrRemoteClient(KettleRemoteClient remoteClient) {
		if (!remoteClient.checkStatus() && remoteclients.containsKey(remoteClient.getHostName())) {
			synchronized (remoteclients) {
				threadPool.scheduleAtFixedRate(new RemoteClientRecoveryStatusDaemon(remoteClient), 1, 10,
						TimeUnit.MINUTES);
				logger.info("Kettle的远程池暂停使用异常Client[" + remoteClient.getHostName() + "]");
			}
		}
	}

	/**
	 * Kettle远端重启动
	 * 
	 * @author Administrator
	 *
	 */
	private class RemoteClientRecoveryStatusDaemon implements Runnable {

		KettleRemoteClient remoteClient = null;

		RemoteClientRecoveryStatusDaemon(KettleRemoteClient remoteClient) {
			this.remoteClient = remoteClient;
		}

		@Override
		public void run() {
			remoteClient.recoveryStatus();
			addRemoteClient(remoteClient);
		}
	}

}
