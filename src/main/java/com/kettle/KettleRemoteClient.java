package com.kettle;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.cluster.TransSplitter;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;

public class KettleRemoteClient {
	/**
	 * 远程服务
	 */
	private final SlaveServer remoteServer;

	/**
	 * 集群配置
	 */
	private final ClusterSchema clusterSchema;

	/**
	 * 资源库
	 */
	private final KettleDatabaseRepository repository;

	/**
	 * 资源池数据库连接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	/**
	 * 运行中的转换记录
	 */
	private List<KettleTransBean> runningTransRecords;

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
					// System.out.println("-Server[" + dbbean.getHostname() +
					// "]->查询Record=" + dbbean.getTransId());
					currentBean = remoteTransStatus(dbbean);
					if (dealRemoteRecord(currentBean)) {
						it.remove();
					}
				} catch (KettleException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			synchronized (newTransRecords) {
				runningTransRecords.addAll(newTransRecords);
				System.out.println(runningTransRecords.size());
				newTransRecords.clear();
				System.out.println(newTransRecords.size());
			}

		}
	};

	public KettleRemoteClient(final KettleDatabaseRepository repository, final SlaveServer remoteServer)
			throws KettleException {
		this.remoteServer = remoteServer;
		this.repository = repository;
		dbRepositoryClient = new KettleDBRepositoryClient(repository);
		runningTransRecords = dbRepositoryClient.allRunningRecord(remoteServer.getHostname());
		String[] cluster = repository.getClustersUsingSlave(remoteServer.getObjectId());
		if (cluster.length > 0) {
			clusterSchema = repository.loadClusterSchema(new LongObjectId(Long.valueOf(cluster[0])),
					Arrays.asList(remoteServer), null);
		} else {
			clusterSchema = null;
		}
		repository.getClusterIDs(false);
	}

	private boolean dealRemoteRecord(KettleTransBean currentBean) throws Exception {
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				if (KettleVariables.RECORD_STATUS_FINISHED.equals(currentBean.getStatus())) {
					/*
					 * 完成
					 */
					dbRepositoryClient.updateTransRecord(currentBean);
					dbRepositoryClient.deleteTransMetaForce(currentBean.getTransId());
					remoteCleanTrans(currentBean.getTransName());
					remoteDRemoveTran(currentBean.getTransName());
					return true;
				} else if (!KettleVariables.RECORD_STATUS_RUNNING.equals(currentBean.getStatus())) {
					dbRepositoryClient.updateTransRecord(currentBean);
					return true;
				} else {
					return false;
				}
			} finally {
				repository.disconnect();
			}
		}
	}

	/**
	 * 远程推送转换
	 * 
	 * @param transMeta
	 * @throws KettleException
	 * @throws Exception
	 */
	protected KettleTransBean remoteSendTrans(TransMeta transMeta) throws KettleException {
		TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
		transExecutionConfiguration.setRemoteServer(remoteServer);
		transExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		transExecutionConfiguration.setPassingExport(false);
		transExecutionConfiguration.setExecutingRemotely(true);
		transExecutionConfiguration.setRepository(repository);
		transExecutionConfiguration.setExecutingLocally(false);
		String runID = Trans.sendToSlaveServer(transMeta, transExecutionConfiguration, repository,
				repository.getMetaStore());
		KettleTransBean kettleTransBean = new KettleTransBean();
		kettleTransBean.setRunID(runID);
		kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		kettleTransBean.setTransName(transMeta.getName());
		kettleTransBean.setHostname(remoteServer.getHostname());
		/*
		 * 插入
		 */
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				dbRepositoryClient.saveTransMeta(transMeta);
				kettleTransBean.setTransId(Long.valueOf(transMeta.getObjectId().getId()));
				dbRepositoryClient.insertTransRecord(kettleTransBean);
			} catch (Exception ex) {
				try {
					remoteDRemoveTran(transMeta.getName());
				} catch (Exception e) {
					e.printStackTrace();
				}
				throw new KettleException("持久化转换发生异常", ex);
			} finally {
				repository.disconnect();
			}
		}
		synchronized (newTransRecords) {
			newTransRecords.add(kettleTransBean);
		}
		return kettleTransBean;
	}

	/**
	 * 远程推送转换
	 * 
	 * @param transMeta
	 * @throws KettleException
	 * @throws Exception
	 */
	protected KettleTransBean clusterSendTrans(TransMeta transMeta) throws KettleException {
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
		transExecutionConfiguration.setRepository(repository);
		transMeta.setSlaveServers(Arrays.asList(remoteServer));
		TransSplitter transSplitter = Trans.executeClustered(transMeta, transExecutionConfiguration);
		KettleTransBean kettleTransBean = new KettleTransBean();
		kettleTransBean.setRunID(transSplitter.getClusteredRunId());
		kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		kettleTransBean.setTransName(transMeta.getName());
		kettleTransBean.setHostname(remoteServer.getHostname());
		/*
		 * 插入
		 */
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				dbRepositoryClient.saveTransMeta(transMeta);
				kettleTransBean.setTransId(Long.valueOf(transMeta.getObjectId().getId()));
				dbRepositoryClient.insertTransRecord(kettleTransBean);
			} catch (Exception ex) {
				try {
					remoteDRemoveTran(transMeta.getName());
				} catch (Exception e) {
					e.printStackTrace();
				}
				throw new KettleException("持久化转换发生异常", ex);
			} finally {
				repository.disconnect();
			}
		}
		synchronized (newTransRecords) {
			newTransRecords.add(kettleTransBean);
		}
		return kettleTransBean;
	}

	/**
	 * 清理运行的Trans, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	protected void remoteCleanTrans(String transName) throws KettleException, Exception {
		remoteServer.cleanupTransformation(transName, null);
	}

	/**
	 * 远程启动, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	protected void remoteStartTrans(String transName) throws KettleException, Exception {
		WebResult result = remoteServer.startTransformation(transName, null);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("转换[" + transName + "]启动失败!");
		}
	}

	/**
	 * 远程停止, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	protected void remoteStopTrans(String transName) throws KettleException, Exception {
		WebResult result = remoteServer.stopTransformation(transName, null);
		if (!"OK".equals(result.getResult())) {
			throw new KettleException("转换[" + transName + "]停止失败!");
		}
	}

	/**
	 * 远程查询状态
	 * 
	 * @param transName
	 * @return
	 * @throws KettleException
	 * @throws Exception
	 */
	protected KettleTransBean remoteTransStatus(KettleTransBean bean) throws KettleException, Exception {
		SlaveServerTransStatus slaveServerStatus = remoteServer.getTransStatus(bean.getTransName(), null, 0);
		System.out.println("Server[" + remoteServer.getHostname() + "]转换[" + bean.getTransName() + "]状态为:"
				+ slaveServerStatus.getStatusDescription());
		if (slaveServerStatus == null || slaveServerStatus.getStatusDescription() == null) {
			bean.setStatus(KettleVariables.RECORD_STATUS_ERROR);
		} else if (slaveServerStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			bean.setStatus(KettleVariables.RECORD_STATUS_ERROR);
		} else if ("Finished".equalsIgnoreCase(slaveServerStatus.getStatusDescription())) {
			for (StepStatus stepStatus : slaveServerStatus.getStepStatusList()) {
				if (!"Finished".equalsIgnoreCase(stepStatus.getStatusDescription())) {
					bean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
					return bean;
				}
			}
			bean.setStatus(KettleVariables.RECORD_STATUS_FINISHED);
		} else {
			bean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		}
		return bean;
	}

	/**
	 * 远程删除Tran
	 *
	 * @param name
	 * @return
	 * @throws Exception
	 */
	protected void remoteDRemoveTran(String transName) throws Exception {
		remoteServer.removeTransformation(transName, null);
	}

	public String getHostName() {
		return this.remoteServer.getHostname();
	}

	protected Runnable deamon() {
		return deamon;
	}

	public ClusterSchema getClusterSchema() {
		return clusterSchema;
	}
}
