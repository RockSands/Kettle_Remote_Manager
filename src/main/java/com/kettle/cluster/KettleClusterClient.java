package com.kettle.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;
import org.pentaho.di.www.SlaveServerTransStatus;

import com.kettle.KettleDBRepositoryClient;
import com.kettle.KettleTransBean;
import com.kettle.KettleTransSplitBean;
import com.kettle.KettleVariables;

public class KettleClusterClient {

	private final String clustername;

	private final ClusterSchema clusterSchema;

	private SlaveServer masterSlave;
	/*
	 * 所有Slave,包括Master
	 */
	private final Map<String, SlaveServer> slaves = new HashMap<String, SlaveServer>();

	// private final KettleDatabaseRepository repository;

	/**
	 * 资源池数据库连接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	/**
	 * 运行中的转换记录
	 */
	private List<KettleTransBean> runningTransRecords = new LinkedList<KettleTransBean>();

	/**
	 * 新的转换记录
	 */
	private List<KettleTransBean> newTransRecords = new LinkedList<KettleTransBean>();

	private Runnable deamon = new Runnable() {
		@Override
		public void run() {
			KettleTransBean currentBean = null;
			KettleTransBean dbbean = null;
			for (Iterator<KettleTransBean> it = runningTransRecords.iterator(); it.hasNext();) {
				try {
					dbbean = it.next();
					currentBean = clusterTransStatus(dbbean);
					if (dealClusterRecord(currentBean)) {
						it.remove();
					}
				} catch (KettleException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			syncRecord();
		}
	};

	private void syncRecord() {
		synchronized (newTransRecords) {
			runningTransRecords.addAll(newTransRecords);
			newTransRecords.clear();
		}
	}

	private boolean dealClusterRecord(KettleTransBean currentBean) throws Exception {
		// synchronized (repository) {
		// repository.connect("admin", "admin");
		// try {
		if (KettleVariables.RECORD_STATUS_FINISHED.equals(currentBean.getStatus())) {
			/*
			 * 完成
			 */
			dbRepositoryClient.updateTransRecord(currentBean);
			return true;
		} else if (!KettleVariables.RECORD_STATUS_RUNNING.equals(currentBean.getStatus())) {
			dbRepositoryClient.updateTransRecord(currentBean);
			return true;
		} else {
			return false;
		}
		// } finally {
		// repository.disconnect();
		// }
		// }
	}

	private KettleTransBean clusterTransStatus(KettleTransBean dbbean) throws Exception {
		SlaveServer slave = null;
		for (KettleTransSplitBean splitBean : dbbean.getClusterSplits()) {
			if (!KettleVariables.RECORD_STATUS_FINISHED.equals(splitBean.getStatus())) {
				slave = slaves.get(splitBean.getHostname());
				if (slave == null) {
					splitBean.setStatus(KettleVariables.RECORD_STATUS_ERROR);
					dbbean.setStatus(KettleVariables.RECORD_STATUS_ERROR);
					return dbbean;
				}
				splitBean.setStatus(getSlaveTransStatus(slave, splitBean.getTransName()));
				if (KettleVariables.RECORD_STATUS_FINISHED.equals(splitBean.getStatus())) {
					dbRepositoryClient.updateClusterTransSplit(splitBean);
				}
			}
		}
		String status = KettleVariables.RECORD_STATUS_FINISHED;
		for (KettleTransSplitBean splitBean : dbbean.getClusterSplits()) {
			if (KettleVariables.RECORD_STATUS_RUNNING.equals(splitBean.getStatus())) {
				if (status == null || !KettleVariables.RECORD_STATUS_ERROR.equals(status)) {
					status = KettleVariables.RECORD_STATUS_RUNNING;
				}
			}
			if (KettleVariables.RECORD_STATUS_ERROR.equals(splitBean.getStatus())) {
				status = KettleVariables.RECORD_STATUS_ERROR;
			}
		}
		dbbean.setStatus(status);
		return dbbean;
	}

	private String getSlaveTransStatus(SlaveServer slave, String transName) {
		try {
			SlaveServerTransStatus slaveServerStatus = slave.getTransStatus(transName, "", 0);
			System.out.println("=Server[" + slave.getHostname() + "],Split[" + transName + "] Status =>"
					+ (slaveServerStatus != null ? slaveServerStatus.getStatusDescription() : "NULL"));
			if (slaveServerStatus == null || slaveServerStatus.getStatusDescription() == null) {
				return KettleVariables.RECORD_STATUS_ERROR;
			} else if (slaveServerStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
				return KettleVariables.RECORD_STATUS_ERROR;
			} else if ("Finished".equalsIgnoreCase(slaveServerStatus.getStatusDescription())) {
				// for (StepStatus stepStatus :
				// slaveServerStatus.getStepStatusList()) {
				// if
				// (!"Finished".equalsIgnoreCase(stepStatus.getStatusDescription()))
				// {
				// bean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
				// return bean;
				// }
				// }
				return KettleVariables.RECORD_STATUS_FINISHED;
			} else {
				return KettleVariables.RECORD_STATUS_RUNNING;
			}
		} catch (Exception e) {
			return KettleVariables.RECORD_STATUS_ERROR;
		}
	}

	public KettleClusterClient(final KettleDatabaseRepository repository, ClusterSchema clusterSchema)
			throws KettleException {
		// this.repository = repository;
		this.clustername = clusterSchema.getName();
		this.clusterSchema = clusterSchema;
		dbRepositoryClient = new KettleDBRepositoryClient(repository);
		for (SlaveServer server : clusterSchema.getSlaveServers()) {
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			if (server.isMaster()) {
				masterSlave = server;
			}
		}
	}

	public KettleTransBean clusterSendTrans(TransMeta transMeta) throws KettleException {
		TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
		transExecutionConfiguration.setClusterPosting(true);
		transExecutionConfiguration.setClusterPreparing(true);
		transExecutionConfiguration.setClusterStarting(true);
		transExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		transExecutionConfiguration.setClusterShowingTransformation(false);
		transExecutionConfiguration.setSafeModeEnabled(false);
		transExecutionConfiguration.setExecutingRemotely(false);
		transExecutionConfiguration.setExecutingLocally(false);
		transExecutionConfiguration.setExecutingClustered(true);
		transMeta.setSlaveServers(Arrays.asList(masterSlave));
		TransSplitter transSplitter = Trans.executeClustered(transMeta, transExecutionConfiguration);
		KettleTransBean kettleTransBean = new KettleTransBean();
		kettleTransBean.setRunID(transSplitter.getClusteredRunId());
		kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		kettleTransBean.setTransName(transMeta.getName());
		kettleTransBean.setHostname(masterSlave.getHostname());
		kettleTransBean.setClusterName(clustername);
		KettleTransSplitBean bean = null;
		for (Map.Entry<SlaveServer, TransMeta> entry : transSplitter.getSlaveTransMap().entrySet()) {
			bean = new KettleTransSplitBean();
			bean.setHostname(entry.getKey().getHostname());
			bean.setTransName(entry.getValue().getName());
			bean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
			kettleTransBean.getClusterSplits().add(bean);
			if (!slaves.containsKey(entry.getKey().getHostname())) {
				slaves.put(entry.getKey().getHostname(), entry.getKey());
			}
		}
		synchronized (newTransRecords) {
			newTransRecords.add(kettleTransBean);
		}
		/*
		 * 插入
		 */
		// synchronized (repository) {
		// repository.connect("admin", "admin");
		try {
			dbRepositoryClient.saveTransMeta(transMeta);
			kettleTransBean.setTransId(Long.valueOf(transMeta.getObjectId().getId()));
			dbRepositoryClient.insertClusterRecord(kettleTransBean);
		} catch (Exception ex) {
			try {
				clusterRemoveTran(transMeta.getName());
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new KettleException("持久化转换发生异常", ex);
			// } finally {
			// repository.disconnect();
			// }
		}
		return kettleTransBean;
	}

	/**
	 * 远程删除Tran
	 *
	 * @param name
	 * @return
	 * @throws Exception
	 */
	protected void clusterRemoveTran(String transName) throws Exception {

	}

	public Runnable deamon() {
		return deamon;
	}

	public ClusterSchema getClusterSchema() {
		return clusterSchema;
	}
}
