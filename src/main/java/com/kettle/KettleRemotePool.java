package com.kettle;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

/**
 * 保存远程运行池
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {
	/**
	 * 
	 */
	private final Collection<KettleRemoteClient> remoteclients;

	/**
	 * 线程池
	 */
	private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	public KettleRemotePool(final KettleDatabaseRepository repository, List<SlaveServer> slaveServers)
			throws KettleException {
		remoteclients = new LinkedList<KettleRemoteClient>();
		KettleRemoteClient remoteClient = null;
		for (SlaveServer server : slaveServers) {
			if (server.isMaster()) {
				server.getLogChannel().setLogLevel(LogLevel.ERROR);
				remoteClient = new KettleRemoteClient(repository, server);
				remoteclients.add(remoteClient);
				threadPool.scheduleAtFixedRate(remoteClient.deamon(), 5, 10, TimeUnit.SECONDS);
			}
		}
		if (remoteclients.isEmpty()) {
			throw new RuntimeException("KettleRemotePool初始化失败,未找到可用的远端服务!");
		}
	}

	/**
	 * @return
	 */
	private KettleRemoteClient nextClient() {
		synchronized (remoteclients) {
			KettleRemoteClient client = ((LinkedList<KettleRemoteClient>) remoteclients).removeFirst();
			remoteclients.add(client);
			return client;
		}
	}

	/**
	 * 远程发送并执行
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 * @throws KettleException
	 */
	public KettleTransBean remoteSendTrans(TransMeta transMeta) throws KettleException, Exception {
		return nextClient().remoteSendTrans(transMeta);
	}
}
