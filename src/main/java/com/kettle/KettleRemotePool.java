package com.kettle;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
	 * 资源池数据库连接
	 */
	private static KettleDBRepositoryClient dbRepositoryClient;

	public KettleRemotePool(final KettleDatabaseRepository repository, List<SlaveServer> slaveServers)
			throws KettleException {
		remoteclients = new LinkedList<KettleRemoteClient>();
		for (SlaveServer server : slaveServers) {
			if (server.isMaster()) {
				server.getLogChannel().setLogLevel(LogLevel.ERROR);
				remoteclients.add(new KettleRemoteClient(repository, server));
			}
		}
		if (remoteclients.isEmpty()) {
			throw new RuntimeException("KettleRemotePool初始化失败,未找到可用的远端服务!");
		}
		dbRepositoryClient = new KettleDBRepositoryClient(repository);
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
