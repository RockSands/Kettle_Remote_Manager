package com.kettle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.ClusterSchema;
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
	private final Map<String, KettleRemoteClient> remoteclients;

	/**
	 * 主机名称
	 */
	private final String[] hostnameArr;

	/**
	 * 集群与主机的对应
	 */
	private final Map<String, String> clusterHostMap;

	/**
	 * 保证第一次为第一个
	 */
	private int index = 0;

	/**
	 * 线程池
	 */
	private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	public KettleRemotePool(final KettleDatabaseRepository repository, List<SlaveServer> slaveServers)
			throws KettleException {
		remoteclients = new HashMap<String, KettleRemoteClient>();
		KettleRemoteClient remoteClient = null;
		int index = 0;
		clusterHostMap = new HashMap<String, String>();
		for (SlaveServer server : slaveServers) {
			if (server.isMaster()) {
				server.getLogChannel().setLogLevel(LogLevel.ERROR);
				remoteClient = new KettleRemoteClient(repository, server);
				remoteclients.put(remoteClient.getHostName(), remoteClient);
				// i作为延迟避免集中操作
				threadPool.scheduleAtFixedRate(remoteClient.deamon(), 10 + 5 * (index++), 30, TimeUnit.SECONDS);
				if (remoteClient.getClusterSchema() != null) {
					clusterHostMap.put(remoteClient.getClusterSchema().getName(), server.getHostname());
				}
			}
		}
		hostnameArr = remoteclients.keySet().toArray(new String[0]);
		if (remoteclients.isEmpty()) {
			throw new RuntimeException("KettleRemotePool初始化失败,未找到可用的远端服务!");
		}
	}

	public synchronized String getOneClient() {
		if (index >= hostnameArr.length) {
			index = 0;
		}
		return hostnameArr[index++];
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
		return this.remoteSendTrans(transMeta, getOneClient());
	}

	/**
	 * 远程发送并执行--指定集群必须使用此方法
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 * @throws KettleException
	 */
	public KettleTransBean remoteSendTrans(TransMeta transMeta, String hostName) throws KettleException, Exception {
		return remoteclients.get(hostName).remoteSendTrans(transMeta);
	}

	/**
	 * 远程发送并执行--指定集群必须使用此方法
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 * @throws KettleException
	 */
	public KettleTransBean clusterSendTrans(TransMeta transMeta, String clusterName) throws KettleException, Exception {
		return remoteclients.get(clusterHostMap.get(clusterName)).clusterSendTrans(transMeta);
	}

	/**
	 * 远程发送并执行--指定集群必须使用此方法
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 * @throws KettleException
	 */
	public ClusterSchema getClusterSchema(String clusterName) throws KettleException, Exception {
		return remoteclients.get(clusterHostMap.get(clusterName)).getClusterSchema();
	}
}
