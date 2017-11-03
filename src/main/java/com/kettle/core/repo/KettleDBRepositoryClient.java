
package com.kettle.core.repo;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleRecord;

/**
 * 数据库工具
 * 
 * @author Administrator
 *
 */
public class KettleDBRepositoryClient {
	/**
	 * 资源库
	 */
	private final KettleDatabaseRepository repository;

	/**
	 * 资源路径
	 */
	private RepositoryDirectoryInterface baseDirectory = null;

	public KettleDBRepositoryClient(KettleDatabaseRepository repository) throws KettleException {
		this.repository = repository;
		baseDirectory = repository.findDirectory("/");
	}

	public void connect() {
		if (!repository.isConnected()) {
			try {
				repository.connect(EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_USER"),
						EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_PASSWD"));
			} catch (KettleException e) {
				throw new RuntimeException("Kettle的资源池无法连接!");
			}
		}
	}

	public void reconnect() {
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
	 * 从资源库获取TransMeta
	 *
	 * @param name
	 * @return
	 * @throws KettleException
	 */
	public synchronized TransMeta getTransMeta(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		TransMeta transMeta = repository.loadTransformation(new LongObjectId(transID), null);
		repository.commit();
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobID
	 * @return
	 * @throws KettleException
	 */
	public synchronized JobMeta getJobMeta(long jobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		JobMeta jobMeta = repository.loadJob(new LongObjectId(jobID), null);
		repository.commit();
		return jobMeta;
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @param repositoryDirectory
	 * @throws KettleException
	 */
	public synchronized void saveTransMeta(TransMeta transMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.save(transMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 向资源库保存TransMeta
	 * 
	 * @param repositoryDirectory
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	private void saveTransMetas(List<TransMeta> transMetas) throws KettleException {
		for (TransMeta meta : transMetas) {
			repository.save(meta, "1", Calendar.getInstance(), null, true);
		}
	}

	/**
	 * 向资源库保存TransMeta
	 * 
	 * @param repositoryDirectory
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void saveJobMeta(JobMeta jobMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.save(jobMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 向资源库保存TransMeta
	 * 
	 * @param repositoryDirectory
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	private void saveJobMetas(List<JobMeta> jobMetas) throws KettleException {
		for (JobMeta meta : jobMetas) {
			repository.save(meta, "1", Calendar.getInstance(), null, true);
		}
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void deleteTransMeta(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.deleteTransformation(new LongObjectId(transID));
		repository.commit();
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteJobMeta(long jobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.deleteJob(new LongObjectId(jobID));
		repository.commit();
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void deleteTransMetaForce(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		try {
			TransMeta transMeta = this.getTransMeta(transID);
			repository.deleteTransformation(new LongObjectId(transID));
			if (transMeta != null) {
				for (String databaseName : transMeta.getDatabaseNames()) {
					repository.deleteDatabaseMeta(databaseName);
				}
			}
		} catch (KettleException e) {
		}
	}

	/**
	 * 持久化操作:查询转换记录
	 * 
	 * @throws KettleException
	 */
	public synchronized KettleRecord queryRecord(long id) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = repository.connectionDelegate.getOneRow(KettleVariables.R_JOB_RECORD,
				KettleVariables.R_JOB_RECORD_ID_JOB, new LongObjectId(id));
		if (table == null || table.size() < 1) {
			return null;
		} else {
			KettleRecord job = new KettleRecord();
			job.setId(id);
			job.setName(table.getString(KettleVariables.R_JOB_RECORD_NAME_JOB, null));
			job.setRunID(table.getString(KettleVariables.R_RECORD_ID_RUN, null));
			job.setStatus(table.getString(KettleVariables.R_RECORD_STATUS, null));
			job.setHostname(table.getString(KettleVariables.R_RECORD_HOSTNAME, null));
			job.setErrMsg(table.getString(KettleVariables.R_RECORD_ERRORMSG, null));
			job.setCronExpression(table.getString(KettleVariables.R_RECORD_CRON_EXPRESSION, null));
			job.setCreateTime(table.getDate(KettleVariables.R_RECORD_CREATETIME, null));
			job.setUpdateTime(table.getDate(KettleVariables.R_RECORD_UPDATETIME, null));
			return job;
		}
	}

	/**
	 * 持久化操作:Insert工作记录
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void insertRecord(KettleRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		Date now = new Date();
		record.setCreateTime(now);
		record.setUpdateTime(now);
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_ID_JOB, ValueMetaInterface.TYPE_INTEGER),
				record.getId());
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_NAME_JOB, ValueMetaInterface.TYPE_STRING),
				record.getName());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				record.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_CRON_EXPRESSION, ValueMetaInterface.TYPE_STRING),
				record.getCronExpression());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE),
				record.getCreateTime());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_DATE),
				record.getUpdateTime());
		repository.connectionDelegate.insertTableRow(KettleVariables.R_JOB_RECORD, table);
		repository.commit();
	}

	/**
	 * 插入历史
	 * 
	 * @param record
	 * @throws KettleException
	 */
	private void insertHistory(KettleRecord record) throws KettleException {
		if (record.isFinished() || record.isError()) {
			RowMetaAndData table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_HISTORY_RECORD_ID, ValueMetaInterface.TYPE_INTEGER),
					record.getId());
			table.addValue(new ValueMeta(KettleVariables.R_HISTORY_RECORD_NAME, ValueMetaInterface.TYPE_STRING),
					record.getName());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
					record.getRunID());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					record.getStatus());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
					record.getHostname());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
					record.getErrMsg());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE),
					new Date());
			repository.connectionDelegate.insertTableRow(KettleVariables.R_HISTORY_RECORD, table);
		}
	}

	/**
	 * 更新工作
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void updateRecord(KettleRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		record.setUpdateTime(new Date());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				record.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_DATE),
				record.getUpdateTime());
		repository.connectionDelegate.updateTableRow(KettleVariables.R_JOB_RECORD, KettleVariables.R_JOB_RECORD_ID_JOB,
				table, new LongObjectId(record.getId()));
		insertHistory(record);
		repository.commit();
	}

	/**
	 * 持久化操作:更新转换记录
	 * 
	 * @throws KettleException
	 */
	public synchronized void updateRecords(List<KettleRecord> kettleRecords) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table;
		for (KettleRecord record : kettleRecords) {
			record.setUpdateTime(new Date());
			table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					record.getStatus());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
					record.getRunID());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
					record.getHostname());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
					record.getErrMsg());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_DATE),
					record.getUpdateTime());
			repository.connectionDelegate.updateTableRow(KettleVariables.R_JOB_RECORD,
					KettleVariables.R_JOB_RECORD_ID_JOB, table, new LongObjectId(record.getId()));
			insertHistory(record);
		}
		repository.commit();
	}

	/**
	 * 删除JOB
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteRecord(long jobID) throws KettleException {
		deleteJobAndDependents(jobID);
		String sql = "DELETE FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_ID_JOB
				+ " = ? ";
		repository.connectionDelegate.performDelete(sql, new LongObjectId(jobID));
		repository.commit();
	}

	/**
	 * 获取所有需处理Job任务
	 * 
	 * @param hostname
	 * @return
	 * @throws KettleException
	 */
	public List<KettleRecord> allHandleRecord() throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_ID_JOB + "," + KettleVariables.R_JOB_RECORD_NAME_JOB + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + "," + KettleVariables.R_RECORD_CREATETIME + ","
				+ KettleVariables.R_RECORD_UPDATETIME + "," + KettleVariables.R_RECORD_ERRORMSG + ","
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM " + KettleVariables.R_JOB_RECORD + " WHERE ("
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NOT NULL OR " + KettleVariables.R_RECORD_STATUS
				+ " in ('" + KettleVariables.RECORD_STATUS_RUNNING + "', '" + KettleVariables.RECORD_STATUS_APPLY
				+ "'))";
		List<Object[]> result = null;
		synchronized (this) {
			result = repository.connectionDelegate.getRows(sql, -1);
		}
		List<KettleRecord> kettleJobBeans = new LinkedList<KettleRecord>();
		if (result == null || result.isEmpty()) {
			return kettleJobBeans;
		}
		KettleRecord bean = null;
		for (Object[] record : result) {
			bean = new KettleRecord();
			bean.setId((Long) record[0]);
			bean.setName((String) record[1]);
			bean.setRunID((String) record[2]);
			bean.setStatus((String) record[3]);
			bean.setHostname(record[4] == null ? null : (String) record[4]);
			bean.setCreateTime((Date) record[5]);
			bean.setUpdateTime((Date) record[6]);
			bean.setErrMsg(record[7] == null ? null : (String) record[7]);
			bean.setCronExpression(record[8] == null ? null : (String) record[8]);
			kettleJobBeans.add(bean);
		}
		return kettleJobBeans;
	}

	public KettleDatabaseRepository getRepository() {
		return repository;
	}

	/**
	 * 维护关系
	 * 
	 * @param dependentTrans
	 * @param dependentJobs
	 * @param mainJob
	 * @throws KettleException
	 */
	public synchronized void saveDependentsRelation(List<TransMeta> dependentTrans, List<JobMeta> dependentJobs,
			JobMeta mainJob) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		if (dependentTrans != null && !dependentTrans.isEmpty()) {
			saveTransMetas(dependentTrans);
		}
		if (dependentJobs != null && !dependentJobs.isEmpty()) {
			saveJobMetas(dependentJobs);
		}
		repository.save(mainJob, "1", Calendar.getInstance(), null, true);
		Date now = new Date();
		RowMetaAndData data = null;
		if (dependentTrans != null && !dependentTrans.isEmpty()) {
			for (TransMeta meta : dependentTrans) {
				data = new RowMetaAndData();
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_ID, ValueMetaInterface.TYPE_INTEGER),
						Long.valueOf(mainJob.getObjectId().getId()));
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_INTEGER),
						Long.valueOf(meta.getObjectId().getId()));
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
						KettleVariables.RECORD_TYPE_TRANS);
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
				repository.connectionDelegate.insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
			}
		}
		if (dependentJobs != null && !dependentJobs.isEmpty()) {
			for (JobMeta meta : dependentJobs) {
				data = new RowMetaAndData();
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_ID, ValueMetaInterface.TYPE_INTEGER),
						Long.valueOf(mainJob.getObjectId().getId()));
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_INTEGER),
						Long.valueOf(meta.getObjectId().getId()));
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
						KettleVariables.RECORD_TYPE_JOB);
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
				repository.connectionDelegate.insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
			}
		}
	}

	/**
	 * 持久化操作:Insert工作记录
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void deleteJobAndDependents(long mainJobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		String sql = "SELECT " + KettleVariables.R_RECORD_DEPENDENT_MASTER_ID + ","
				+ KettleVariables.R_RECORD_DEPENDENT_META_ID + "," + KettleVariables.R_RECORD_DEPENDENT_META_TYPE
				+ " FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_ID + " = " + mainJobID;
		List<Object[]> result = null;
		synchronized (this) {
			result = repository.connectionDelegate.getRows(sql, -1);
		}
		String type = null;
		long id = 0;
		for (Object[] dependent : result) {
			type = (String) dependent[2];
			id = (Long) dependent[1];
			if (KettleVariables.RECORD_TYPE_TRANS.equals(type)) {
				deleteTransMeta(id);
			} else if (KettleVariables.RECORD_TYPE_JOB.equals(type)) {
				deleteJobMeta(id);
			}
		}
		deleteJobMeta(mainJobID);
		repository.commit();
	}

	/**
	 * 获取基础路径
	 * 
	 * @return
	 */
	public RepositoryDirectoryInterface getBaseDirectory() {
		return baseDirectory;
	}

	/**
	 * 获取基础路径
	 * 
	 * @return
	 * @throws KettleException
	 */
	public synchronized RepositoryDirectoryInterface createDirectory(String patch) throws KettleException {
		RepositoryDirectoryInterface rei = repository.findDirectory("/" + patch);
		if (rei == null) {
			rei = repository.createRepositoryDirectory(this.baseDirectory, patch);
		}
		return rei;
	}

	/**
	 * 获取所有停止了的Record
	 * 
	 * @return
	 * @throws KettleDatabaseException
	 */
	public List<KettleRecord> allStopRecord() throws KettleDatabaseException {
		if (!repository.isConnected()) {
			connect();
		}
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_ID_JOB + "," + KettleVariables.R_JOB_RECORD_NAME_JOB + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + "," + KettleVariables.R_RECORD_CREATETIME + ","
				+ KettleVariables.R_RECORD_UPDATETIME + "," + KettleVariables.R_RECORD_ERRORMSG + ","
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM " + KettleVariables.R_JOB_RECORD + " WHERE "
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NULL AND " + KettleVariables.R_RECORD_STATUS
				+ " in ('" + KettleVariables.RECORD_STATUS_FINISHED + "', '" + KettleVariables.RECORD_STATUS_ERROR + "');";
		List<Object[]> result = null;
		synchronized (this) {
			result = repository.connectionDelegate.getRows(sql, -1);
		}
		List<KettleRecord> kettleJobBeans = new LinkedList<KettleRecord>();
		if (result == null || result.isEmpty()) {
			return kettleJobBeans;
		}
		KettleRecord bean = null;
		for (Object[] record : result) {
			bean = new KettleRecord();
			bean.setId((Long) record[0]);
			bean.setName((String) record[1]);
			bean.setRunID((String) record[2]);
			bean.setStatus((String) record[3]);
			bean.setHostname(record[4] == null ? null : (String) record[4]);
			bean.setCreateTime((Date) record[5]);
			bean.setUpdateTime((Date) record[6]);
			bean.setErrMsg(record[7] == null ? null : (String) record[7]);
			bean.setCronExpression(record[8] == null ? null : (String) record[8]);
			kettleJobBeans.add(bean);
		}
		return kettleJobBeans;
	}
}
