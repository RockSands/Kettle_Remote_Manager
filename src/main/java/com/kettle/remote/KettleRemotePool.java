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
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

import com.kettle.KettleTransBean;

/**
 * 保存远程运行池
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {
	private final Map<String, KettleRemoteClient> remoteclients;

	private final String[] hostnameArr;

	/**
	 * 保证第一次为第一个
	 */
	private int index = 0;

	/**
	 * 线程池
	 */
	private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	/**
	 * @param repository
	 * @param includeServers
	 * @param excludeServers
	 * @throws KettleException
	 */
	public KettleRemotePool(final KettleDatabaseRepository repository, List<String> includeHostNames,
			List<String> excludeHostNames) throws KettleException {
		remoteclients = new HashMap<String, KettleRemoteClient>();
		KettleRemoteClient remoteClient = null;
		for (SlaveServer server : repository.getSlaveServers()) {
			if (excludeHostNames != null && excludeHostNames.contains(server.getHostname())) {
				continue;
			}
			if (includeHostNames != null && !includeHostNames.contains(server.getHostname())) {
				continue;
			}
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			remoteClient = new KettleRemoteClient(repository, server);
			if (remoteclients.containsKey(remoteClient.getHostName())) {
				throw new KettleException("远程池启动失败,存在Hostname重复的主机!");
			}
			remoteclients.put(remoteClient.getHostName(), remoteClient);
			// i作为延迟避免集中操作
			threadPool.scheduleAtFixedRate(remoteClient.deamon(), 10 + 5 * (remoteclients.size()), 30,
					TimeUnit.SECONDS);
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
		return remoteSendTrans(transMeta, getOneClient());
	}

	/**
	 * 指定主机名执行
	 * 
	 * @param transMeta
	 * @param hostName
	 * @return
	 * @throws KettleException
	 * @throws Exception
	 */
	public KettleTransBean remoteSendTrans(TransMeta transMeta, String hostName) throws KettleException, Exception {
		return remoteclients.get(hostName).remoteSendTrans(transMeta);
	}
}
