package com.kettle.remote;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import com.kettle.core.bean.KettleJobResult;
import com.kettle.core.bean.KettleTransResult;
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

	/**
	 * 日志
	 */
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
	 * 设备名称
	 */
	private final List<String> hostNames = new LinkedList<String>();

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
		int i = 1;
		for (SlaveServer server : dbRepositoryClient.getRepository().getSlaveServers()) {
			if (excludeHostNames != null && excludeHostNames.contains(server.getHostname())) {
				continue;
			}
			if (includeHostNames != null && !includeHostNames.contains(server.getHostname())) {
				continue;
			}
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			addRemoteClient(new KettleRemoteClient(this, server, 3 * i));
			hostNames.add(server.getHostname());
			i++;
		}
		logger.info("Kettle远程池已经加载Client" + remoteclients.keySet());
		List<KettleJobRecord> jobs = dbRepositoryClient.allHandleJobRecord(hostNames);
		List<KettleTransRecord> trans = dbRepositoryClient.allHandleTransRecord(hostNames);
		for (KettleJobRecord job : jobs) {
			kettleRecordPool.addPrioritizeRecord(job);
		}
		for (KettleTransRecord tran : trans) {
			kettleRecordPool.addPrioritizeRecord(tran);
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
	 * 申请并执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleTransResult applyTransMeta(TransMeta transMeta) throws KettleException {
		try {
			dbRepositoryClient.saveTransMeta(transMeta);
			KettleTransRecord record = new KettleTransRecord(transMeta);
			record.setId(Long.valueOf(transMeta.getObjectId().getId()));
			record.setName(transMeta.getName());
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			dbRepositoryClient.insertTransRecord(record);
			kettleRecordPool.addRecord(record);
			KettleTransResult result = new KettleTransResult();
			result.setStatus(record.getStatus());
			result.setTransID(record.getId());
			return result;
		} catch (KettleException e) {
			logger.error("Trans[" + transMeta.getName() + "]持久化发生异常!", e);
			throw new KettleException("Trans[" + transMeta.getName() + "]持久化发生异常!");
		}
	}

	/**
	 * 接受并执行作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleJobResult applyJobMeta(JobMeta jobMeta) throws KettleException {
		JobEntryCopy jec = jobMeta.getStart();
		if (jec == null) {
			throw new KettleException("JobMeta[" + jobMeta.getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("Kettle远程池仅受理即时任务!");
		}
		dbRepositoryClient.saveJobMeta(jobMeta);
		KettleJobRecord record = new KettleJobRecord(jobMeta);
		record.setId(Long.valueOf(jobMeta.getObjectId().getId()));
		record.setName(jobMeta.getName());
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		dbRepositoryClient.insertJobRecord(record);
		kettleRecordPool.addRecord(record);
		KettleJobResult result = new KettleJobResult();
		result.setStatus(record.getStatus());
		result.setJobID(record.getId());
		return result;
	}

	/**
	 * 执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleTransResult excuteTransMeta(long transID) throws KettleException {
		KettleTransRecord record = dbRepositoryClient.queryTransRecord(transID);
		if (record == null || record.getKettleMeta() == null) {
			throw new KettleException("Trans[" + transID + "]未为执行Apply操作!");
		}
		if (record.isApply() || record.isRunning()) {
			throw new KettleException("Trans[" + transID + "]仍在执行,并未完成,无法重新执行!");
		}
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setErrMsg(null);
		record.setRunID(null);
		kettleRecordPool.addRecord(record);
		KettleTransResult result = new KettleTransResult();
		result.setStatus(record.getStatus());
		result.setTransID(record.getId());
		return result;
	}

	/**
	 * 执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleJobResult excuteJobMeta(long jobID) throws KettleException {
		KettleJobRecord record = dbRepositoryClient.queryJobRecord(jobID);
		if (record == null || record.getKettleMeta() == null) {
			throw new KettleException("Job[" + jobID + "]未执行Apply操作!");
		}
		if (record.isApply() || record.isRunning()) {
			throw new KettleException("Job[" + jobID + "]仍在执行,并未完成,无法重新执行!");
		}
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setErrMsg(null);
		record.setRunID(null);
		kettleRecordPool.addRecord(record);
		KettleJobResult result = new KettleJobResult();
		result.setStatus(record.getStatus());
		result.setJobID(record.getId());
		return result;
	}

	/**
	 * 添加Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void addRemoteClient(KettleRemoteClient remoteClient) {
		if (remoteclients.containsKey(remoteClient.getHostName())) {
			logger.error("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]失败,该主机已存在!");
		} else {
			remoteclients.put(remoteClient.getHostName(), remoteClient);
			logger.info("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]成功!");
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
