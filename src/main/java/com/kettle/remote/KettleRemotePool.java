package com.kettle.remote;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

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
	 * 验证池子是否可用
	 * 
	 * @return
	 */
	public boolean checkRemotePoolStatus() {
		return remoteclients.size() > 0;
	}

	/**
	 * 获取下一个FreeClient
	 * 
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
			kettleRecordPool.addRecord(record);
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
	 * @return
	 */
	public KettleRecordPool getKettleRecordPool() {
		return kettleRecordPool;
	}

	/**
	 * @return
	 */
	public KettleDBRepositoryClient getDbRepositoryClient() {
		return dbRepositoryClient;
	}

	/**
	 * @return
	 */
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
}
