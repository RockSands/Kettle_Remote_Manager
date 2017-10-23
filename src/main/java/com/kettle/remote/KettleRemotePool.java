package com.kettle.remote;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.repo.KettleDBRepositoryClient;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordPool;
import com.kettle.record.KettleTransRecord;

/**
 * 保存远程运行池
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {

	Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	/**
	 * 远程连接
	 */
	private final ConcurrentMap<String, KettleRemoteClient> remoteclients;

	/**
	 * 资源链接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool kettleRecordPool = new KettleRecordPool();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(10);

	/**
	 * hostNames
	 */
	private final Queue<String> hostNames = new LinkedBlockingQueue<String>();

	/**
	 * @param dbRepositoryClient
	 * @param includeServers
	 * @param excludeServers
	 * @throws KettleException
	 */
	public KettleRemotePool(final KettleDBRepositoryClient dbRepositoryClient, List<String> includeHostNames,
			List<String> excludeHostNames) throws KettleException {
		this.remoteclients = new ConcurrentHashMap<String, KettleRemoteClient>();
		this.dbRepositoryClient = dbRepositoryClient;
		for (SlaveServer server : dbRepositoryClient.getRepository().getSlaveServers()) {
			if (excludeHostNames != null && excludeHostNames.contains(server.getHostname())) {
				continue;
			}
			if (includeHostNames != null && !includeHostNames.contains(server.getHostname())) {
				continue;
			}
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			addRemoteClient(new KettleRemoteClient(this, server));
			hostNames.add(server.getHostname());
		}
	}

	/**
	 * @return
	 */
	public KettleRemoteClient getNextFreeClient() {
		String hostName = null;
		for (int i = 0, size = hostNames.size(); i < size; i++) {
			hostName = hostNames.poll();
			hostNames.add(hostName);
			KettleRemoteClient client = remoteclients.get(hostName);
			if (client != null && remoteclients.get(hostName).freeDaemonCount() > 0) {
				return remoteclients.get(hostName);
			}
		}
		return null;
	}

	/**
	 * 验证池子是否可用
	 * 
	 * @return
	 */
	public boolean getRemotePoolStatus() {
		return remoteclients.size() > 0;
	}

	/**
	 * 验证池子是否可用
	 * 
	 * @return
	 * @throws KettleException
	 */
	public void checkRemotePool() throws KettleException {
		if (!getRemotePoolStatus()) {
			throw new KettleException("Kettle远程池无法使用,原因:没有可用的远端节点!");
		}
	}

	/**
	 * 分配任务
	 * 
	 * @param record
	 */
	public void distributeRecord(KettleTransRecord record) {
		// 放入队列池
		kettleRecordPool.addRecords(record);
		KettleRemoteClient client = getNextFreeClient();
		client.newRecordApply();
	}

	public void remoteSendRecord(KettleRemoteClient remoteClient, KettleRecord record) {
		if (KettleJobRecord.class.isInstance(record)) {
			KettleJobRecord job = (KettleJobRecord) record;
			remoteClient.remoteSendJob(job.getKettleMeta());
		} else if (KettleTransRecord.class.isInstance(record)) {
			KettleTransRecord trans = (KettleTransRecord) record;
			remoteClient.remoteSendTrans(trans);
		}
	}

	/**
	 * 接受转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleTransRecord applyTransMeta(TransMeta transMeta) throws KettleException {
		try {
			dbRepositoryClient.saveTransMeta(transMeta);
			KettleTransRecord record = new KettleTransRecord(transMeta);
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			dbRepositoryClient.insertTransRecord(record);
			distributeRecord(record);
			return record;
		} catch (KettleException e) {
			logger.error("Trans[" + transMeta.getName() + "]持久化发生异常!", e);
			throw new KettleException("Trans[" + transMeta.getName() + "]持久化发生异常!");
		}
	}

	/**
	 * 接受作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleJobRecord applyJobMeta(JobMeta jobMeta) throws KettleException {
		KettleJobRecord record = new KettleJobRecord(jobMeta);
		JobEntryCopy jec = jobMeta.getStart();
		if (jec == null) {
			throw new KettleException("JobMeta[" + jobMeta.getName() + "]没有定义Start,无法处理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (!jobStart.isRepeat() && jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("Kettle远程池仅接受重复或即时任务!");
		}
		record.setType(
				jobStart.isRepeat() ? KettleVariables.JOB_RECORD_TYPE_REPEAT : KettleVariables.JOB_RECORD_TYPE_ONCE);
		jobStart.isRepeat();
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setName(jobMeta.getName());
		try {
			dbRepositoryClient.saveJobMeta(jobMeta);
			record.setId(Long.valueOf(jobMeta.getObjectId().getId()));
			dbRepositoryClient.insertJobRecord(record);
			kettleRecordPool.addRecords(record);
		} catch (KettleException e) {
			logger.error("Trans[" + jobMeta.getName() + "]持久化发生异常!", e);
			throw new KettleException("Trans[" + jobMeta.getName() + "]持久化发生异常!");
		}
		return record;
	}

	/**
	 * 添加Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void addRemoteClient(KettleRemoteClient remoteClient) {
		if (!remoteClient.checkRemoteStatus()) {
			logger.error("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]失败,状态不可用!");
			dealErrRemoteClient(remoteClient.getHostName());
		} else {
			if (remoteclients.containsKey(remoteClient.getHostName())) {
				logger.error("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]失败,该主机已存在!");
			} else {
				remoteclients.put(remoteClient.getHostName(), remoteClient);
				logger.info("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]成功!");
			}
		}
	}

	/**
	 * 处理Kettle异常远端
	 * 
	 * @param remoteClient
	 */
	protected void dealErrRemoteClient(String hostName) {
		KettleRemoteClient remoteClient = remoteclients.get(hostName);
		if (remoteClient != null && !remoteClient.checkRemoteStatus()) {
			remoteclients.remove(hostName);
			logger.info("Kettle的远程池暂停使用异常Client[" + remoteClient.getHostName() + "],开始定时查看!");
			threadPool.schedule(new RemoteClientRecoveryStatusDaemon(remoteClient), 20, TimeUnit.SECONDS);
		}
	}

	/**
	 * 在守护进程中
	 * 
	 * @param remoteClientRecoveryStatusDaemon
	 */
	private void recoveryRemoteClient(RemoteClientRecoveryStatusDaemon remoteClientRecoveryStatusDaemon) {
		if (remoteClientRecoveryStatusDaemon.remoteClient.checkRemoteStatus()) {
			logger.info(
					"Kettle的远程池添加Client[" + remoteClientRecoveryStatusDaemon.remoteClient.getHostName() + "]状态为可用!");
			addRemoteClient(remoteClientRecoveryStatusDaemon.remoteClient);
		} else {
			threadPool.schedule(remoteClientRecoveryStatusDaemon, 10, TimeUnit.MINUTES);
		}
	}

	public KettleRecordPool getKettleRecordPool() {
		return kettleRecordPool;
	}

	public KettleDBRepositoryClient getDbRepositoryClient() {
		return dbRepositoryClient;
	}

	public KettleDatabaseRepository getDbRepository() {
		return dbRepositoryClient.getRepository();
	}

	/**
	 * 获取所有Client
	 * 
	 * @return
	 */
	public Collection<KettleRemoteClient> getAllRemoteclients() {
		return remoteclients.values();
	}

	/**
	 * Kettle远端重启动
	 * 
	 * @author Administrator
	 *
	 */
	protected class RemoteClientRecoveryStatusDaemon implements Runnable {

		KettleRemoteClient remoteClient = null;

		int dealCount = 0;

		RemoteClientRecoveryStatusDaemon(KettleRemoteClient remoteClient) {
			this.remoteClient = remoteClient;
		}

		@Override
		public void run() {
			remoteClient.recoveryStatus();
			recoveryRemoteClient(this);
			dealCount++;
		}

		public int getDealCount() {
			return dealCount;
		}
	}
}
