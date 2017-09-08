package com.kettle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;

public class KettleRemoteClient {
	/**
	 * 远程服务
	 */
	private final SlaveServer remoteServer;

	/**
	 * 资源库
	 */
	private final KettleDatabaseRepository repository;

	/**
	 * 资源池数据库连接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	private ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	protected KettleRemoteClient(final KettleDatabaseRepository repository, final SlaveServer remoteServer) {
		this.remoteServer = remoteServer;
		this.repository = repository;
		dbRepositoryClient = new KettleDBRepositoryClient(repository);
	}

	/**
	 * 远程推送转换
	 * 
	 * @param transMeta
	 * @throws KettleException
	 * @throws Exception
	 */
	public KettleTransBean remoteSendTrans(TransMeta transMeta) throws KettleException, Exception {
		TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
		transExecutionConfiguration.setRemoteServer(remoteServer);
		transExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		transExecutionConfiguration.setRepository(repository);
		transExecutionConfiguration.setPassingExport(false);
		transExecutionConfiguration.setExecutingRemotely(true);
		transExecutionConfiguration.setExecutingLocally(false);
		String runID = Trans.sendToSlaveServer(transMeta, transExecutionConfiguration, repository,
				repository.getMetaStore());
		KettleTransBean kettleTransBean = new KettleTransBean();
		kettleTransBean.setRunID(runID);
		kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		kettleTransBean.setTransName(transMeta.getName());
		kettleTransBean.setHostname(remoteServer.getHostname());
		return kettleTransBean;
	}

	/**
	 * 清理运行的Trans, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteCleanTrans(String transName) throws KettleException, Exception {
		remoteServer.cleanupTransformation(transName, null);
	}

	/**
	 * 远程启动, 两个参数必选其一
	 * 
	 * @param transName
	 * @throws KettleException
	 * @throws Exception
	 */
	public void remoteStartTrans(String transName) throws KettleException, Exception {
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
	public void remoteStopTrans(String transName) throws KettleException, Exception {
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
	public KettleTransBean remoteTransStatus(String transName) throws KettleException, Exception {
		SlaveServerTransStatus slaveServerStatus = remoteServer.getTransStatus(transName, null, 0);
		KettleTransBean kettleTransBean = new KettleTransBean();
		kettleTransBean.setTransName(transName);
		if (slaveServerStatus.getStatusDescription().toUpperCase().contains("ERROR")) {
			kettleTransBean.setErrMsg(slaveServerStatus.getErrorDescription());
			kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_ERROR);
		} else if ("Finished".equalsIgnoreCase(slaveServerStatus.getStatusDescription())) {
			for (StepStatus stepStatus : slaveServerStatus.getStepStatusList()) {
				if (!"Finished".equalsIgnoreCase(stepStatus.getStatusDescription())) {
					kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
					return kettleTransBean;
				}
			}
			kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_FINISHED);
		} else {
			System.out.println("转换[" + transName + "]状态为:" + slaveServerStatus.getStatusDescription());
			kettleTransBean.setStatus(KettleVariables.RECORD_STATUS_OTRHER);
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
	public void remoteDRemoveTran(String transName) throws Exception {
		remoteServer.removeTransformation(transName, null);
	}
}
