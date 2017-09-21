package com.kettle.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

import com.kettle.KettleTransBean;

public class KettleClusterPool {
	/**
	 * 集群对象机核
	 */
	private final Map<String, KettleClusterClient> clusterClients;

	/**
	 * 集群名称
	 */
	private final String[] clusterNames;

	private int index = 0;

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	public KettleClusterPool(final KettleDatabaseRepository repository, List<String> includeClusters,
			List<String> excludeClusters) throws KettleException {
		clusterClients = new HashMap<String, KettleClusterClient>();
		ObjectId[] clusterIDS = repository.getClusterIDs(false);
		ClusterSchema clusterSchema = null;
		KettleClusterClient kettleClusterClient = null;
		for (ObjectId clusterID : clusterIDS) {
			clusterSchema = repository.loadClusterSchema(clusterID, Collections.<SlaveServer>emptyList(), null);
			if (excludeClusters != null && excludeClusters.contains(clusterSchema.getName())) {
				continue;
			}
			if (includeClusters != null && !includeClusters.contains(clusterSchema.getName())) {
				continue;
			}
			kettleClusterClient = new KettleClusterClient(repository, clusterSchema);
			clusterClients.put(clusterSchema.getName(), kettleClusterClient);
			// i作为延迟避免集中操作
			threadPool.scheduleAtFixedRate(kettleClusterClient.deamon(), 10 + 5 * (clusterClients.size()), 30,
					TimeUnit.SECONDS);
		}
		clusterNames = clusterClients.keySet().toArray(new String[0]);
	}

	/**
	 * 所有集群名称
	 * 
	 * @return
	 */
	public synchronized List<String> getAllCluster() {
		List<String> clusterNamesList = new ArrayList<String>(clusterClients.keySet());
		return clusterNamesList;
	}

	/**
	 * 获得一个集群名称
	 * 
	 * @return
	 */
	public synchronized String getOneCluster() {
		if (index >= clusterNames.length) {
			index = 0;
		}
		return clusterNames[index++];
	}

	/**
	 * 集群执行
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 * @throws Exception
	 */
	public KettleTransBean clusterSendTrans(TransMeta transMeta) throws KettleException, Exception {
		return clusterSendTrans(transMeta, getOneCluster());
	}

	/**
	 * 集群执行
	 * 
	 * @param transMeta
	 * @param clustername
	 * @return
	 * @throws KettleException
	 * @throws Exception
	 */
	public KettleTransBean clusterSendTrans(TransMeta transMeta, String clustername) throws KettleException, Exception {
		return clusterClients.get(clustername).clusterSendTrans(transMeta);
	}

	/**
	 * 集群配置
	 * 
	 * @param string
	 * @return
	 */
	public ClusterSchema getClusterSchema(String clustername) {
		return clusterClients.get(clustername).getClusterSchema();
	}

}
