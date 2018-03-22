
package com.kettle.core.repo;

import java.util.Calendar;
import java.util.List;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordRelation;

/**
 * Kettle资源库
 * 
 * @author Administrator
 *
 */
public class KettleRepositoryClient {
	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRepositoryClient.class);
	/**
	 * 资源库
	 */
	private final Repository repository;

	/**
	 * 构造器
	 * 
	 * @param repository
	 * @throws KettleException
	 */
	public KettleRepositoryClient(Repository repository) throws KettleException {
		this.repository = repository;
	}

	/**
	 * 获取资源库
	 * 
	 * @return
	 */
	public Repository getRepository() {
		return repository;
	}

	/**
	 * 转变为ObjectId
	 * 
	 * @param id
	 * @return
	 */
	private ObjectId toObjectID(String id) {
		if (KettleFileRepository.class.isInstance(repository)) {// 文件
			return new StringObjectId(id);
		} else { // 数据库
			return new LongObjectId(Long.valueOf(id));
		}
	}

	/**
	 * 连接
	 */
	public synchronized void connect() {
		if (!repository.isConnected()) {
			try {
				repository.connect(EnvUtil.getSystemProperty("KETTLE_REPOSITORY_USER"),
						EnvUtil.getSystemProperty("KETTLE_REPOSITORY_PASSWD"));
			} catch (KettleException e) {
				throw new RuntimeException("Kettle的资源池无法连接!");
			}
		}
	}

	/**
	 * 关闭连接
	 */
	public synchronized void disconnect() {
		if (repository.isConnected()) {
			repository.disconnect();
		}
	}

	/**
	 * 重新连接
	 */
	public synchronized void reconnect() {
		try {
			if (repository.isConnected()) {
				repository.disconnect();
			}
			repository.connect("admin", "admin");
		} catch (KettleException e) {
			throw new RuntimeException("Kettle的资源池无法连接!");
		}
	}

	/**
	 * 获取路径,如果不存在则创建
	 * 
	 * @return
	 * @throws KettleException
	 */
	public synchronized RepositoryDirectoryInterface getDirectory(String path) throws KettleException {
		connect();
		RepositoryDirectoryInterface rdi = repository.findDirectory(path);
		if (rdi == null) {
			rdi = repository.createRepositoryDirectory(repository.findDirectory(""), path);
		}
		return rdi;
	}

	/**
	 * 删除空的目录
	 * 
	 * @throws KettleException
	 */
	public synchronized void deleteEmptyRepoPath() throws KettleException {
		connect();
		List<RepositoryDirectoryInterface> directorys = repository.findDirectory("").getChildren();
		for (RepositoryDirectoryInterface directory : directorys) {
			if (directory.getRepositoryObjects().isEmpty()) {
				repository.deleteRepositoryDirectory(directory);
			}
		}
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @param path
	 * @throws KettleException
	 */
	private synchronized void saveTransMeta(TransMeta transMeta, String path) throws KettleException {
		transMeta.setRepository(repository);
		transMeta.setRepositoryDirectory(getDirectory(path));
		repository.save(transMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 向资源库保存TransMeta
	 * 
	 * @param jobMeta
	 * @param path
	 * @throws KettleException
	 */
	private synchronized void saveJobMeta(JobMeta jobMeta, String path) throws KettleException {
		jobMeta.setRepository(repository);
		jobMeta.setRepositoryDirectory(getDirectory(path));
		repository.save(jobMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 从资源库获取TransMeta
	 *
	 * @param transID
	 * @return
	 * @throws KettleException
	 */
	private TransMeta getTransMeta(String transID) throws KettleException {
		TransMeta transMeta = repository.loadTransformation(toObjectID(transID), null);
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobID
	 * @return
	 * @throws KettleException
	 */
	private JobMeta getJobMeta(String jobID) throws KettleException {
		JobMeta jobMeta = repository.loadJob(toObjectID(jobID), null);
		return jobMeta;
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transID
	 * @throws KettleException
	 */
	private void deleteTransMeta(String transID) throws KettleException {
		repository.deleteTransformation(toObjectID(transID));
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transID
	 * @throws KettleException
	 */
	private void deleteTransMetaNE(String transID) {
		try {
			deleteTransMeta(transID);
		} catch (KettleException e) {
			logger.error("资源池删除Trans[" + transID + "]发生异常", e);
		}
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	private void deleteJobMeta(String jobID) throws KettleException {
		repository.deleteJob(toObjectID(jobID));
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	private void deleteJobMetaNE(String jobID) {
		try {
			deleteJobMeta(jobID);
		} catch (KettleException e) {
			logger.error("资源池删除JOB[" + jobID + "]发生异常", e);
		}
	}

	/**
	 * @param jobEntire
	 * @param path
	 * @throws KettleException
	 */
	public synchronized KettleRecord saveJobEntireDefine(KettleJobEntireDefine jobEntire, String path)
			throws KettleException {
		connect();
		KettleRecord record = new KettleRecord();
		saveJobMeta(jobEntire.getMainJob(), path);
		record.setUuid(jobEntire.getUuid());
		record.setJobid(jobEntire.getMainJob().getObjectId().getId());
		record.setName(jobEntire.getMainJob().getName());
		// 依赖
		KettleRecordRelation relation;
		for (TransMeta transMeta : jobEntire.getDependentTrans()) {
			saveTransMeta(transMeta, path);
			relation = new KettleRecordRelation();
			relation.setMasterUUID(record.getUuid());
			relation.setMetaid(transMeta.getObjectId().getId());
			relation.setType(KettleVariables.RECORD_TYPE_TRANS);
			record.getRelations().add(relation);
		}
		for (JobMeta jobMeta : jobEntire.getDependentJobs()) {
			saveJobMeta(jobMeta, path);
			relation = new KettleRecordRelation();
			relation.setMasterUUID(record.getUuid());
			relation.setMetaid(jobMeta.getObjectId().getId());
			relation.setType(KettleVariables.RECORD_TYPE_JOB);
			record.getRelations().add(relation);
		}
		return record;
	}

	/**
	 * @param record
	 * @throws KettleException
	 */
	public synchronized JobMeta getMainJob(KettleRecord record) throws KettleException {
		connect();
		JobMeta jobMeta = getJobMeta(record.getJobid());
		if (jobMeta == null) {
			throw new KettleException("Kettle资源库未找到Recode[" + record.getUuid() + "],其资源ID为[" + record.getJobid() + "]");
		}
		return jobMeta;
	}

	/**
	 * @param jobEntire
	 * @throws KettleException
	 */
	public synchronized KettleJobEntireDefine getJobEntireDefine(KettleRecord record) throws KettleException {
		connect();
		TransMeta transMeta;
		JobMeta jobMeta;
		KettleJobEntireDefine jobEntire = new KettleJobEntireDefine();
		jobMeta = getJobMeta(record.getJobid());
		if (jobMeta == null) {
			throw new KettleException("Kettle资源库未找到Recode[" + record.getUuid() + "],其资源ID为[" + record.getJobid() + "]");
		}
		jobEntire.setMainJob(jobMeta);
		// 构建依赖
		for (KettleRecordRelation relation : record.getRelations()) {
			if (KettleVariables.RECORD_TYPE_JOB.equals(relation.getType())) {
				jobMeta = getJobMeta(relation.getMetaid());
				if (jobMeta == null) {
					throw new KettleException(
							"Kettle资源库未找到Recode配置[" + record.getUuid() + "]的Job依赖[" + relation.getMetaid() + "]");
				}
				jobEntire.getDependentJobs().add(jobMeta);
			} else if (KettleVariables.RECORD_TYPE_TRANS.equals(relation.getType())) {
				transMeta = getTransMeta(relation.getMetaid());
				if (transMeta == null) {
					throw new KettleException(
							"Kettle资源库未找到Recode配置[" + record.getUuid() + "]的Trans依赖[" + relation.getMetaid() + "]");
				}
				jobEntire.getDependentTrans().add(transMeta);
			}
		}
		return jobEntire;
	}

	/**
	 * 修改路径,jobEntire的MainJob的ObjectID会发生变化
	 * 
	 * @param jobEntire
	 * @throws KettleException
	 */
	public synchronized void moveJobEntireDefine(KettleRecord record, String newPath) throws KettleException {
		connect();
		RepositoryDirectoryInterface newDirectory = getDirectory(newPath);
		ObjectId newID = repository.renameRepositoryDirectory(toObjectID(record.getJobid()), newDirectory, null);
		record.setJobid(newID.getId());
		for (KettleRecordRelation relation : record.getRelations()) {
			newID = repository.renameRepositoryDirectory(toObjectID(relation.getMetaid()), newDirectory, null);
			relation.setMetaid(newID.getId());
		}
	}

	/**
	 * 删除依赖
	 * 
	 * @param relations
	 */
	public synchronized void deleteJobEntireDefine(KettleRecord record) {
		connect();
		deleteJobMetaNE(record.getJobid());
		for (KettleRecordRelation relation : record.getRelations()) {
			if (KettleVariables.RECORD_TYPE_TRANS.equals(relation.getType())) {
				deleteTransMetaNE(relation.getMetaid());
			} else if (KettleVariables.RECORD_TYPE_JOB.equals(relation.getType())) {
				deleteJobMetaNE(relation.getMetaid());
			}
		}
	}

	/**
	 * 获取所有子节点
	 * 
	 * @return
	 * @throws KettleException
	 */
	public synchronized List<SlaveServer> getSlaveServers() throws KettleException {
		connect();
		return this.repository.getSlaveServers();
	}
}
