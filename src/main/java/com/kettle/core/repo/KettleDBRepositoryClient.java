
package com.kettle.core.repo;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleTransRecord;

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
	private RepositoryDirectoryInterface repositoryDirectory = null;

	private static Object lock = new Object();

	public KettleDBRepositoryClient(KettleDatabaseRepository repository) throws KettleException {
		this.repository = repository;
		repositoryDirectory = repository.findDirectory("");
	}

	public void connect() {
		synchronized (lock) {
			if (!repository.isConnected()) {
				try {
					repository.connect(EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_USER"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_PASSWD"));
				} catch (KettleException e) {
					throw new RuntimeException("Kettle的资源池无法连接!");
				}
			}
		}
	}

	public void reconnect() {
		synchronized (lock) {
			try {
				if (repository.isConnected()) {
					repository.disconnect();
				}
				repository.connect("admin", "admin");
			} catch (KettleException e) {
				throw new RuntimeException("Kettle的资源池无法连接!");
			}
		}
	}

	/**
	 * 从资源库获取TransMeta
	 *
	 * @param name
	 * @return
	 * @throws KettleException
	 */
	public TransMeta getTransMeta(String transName) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		ObjectId transformationID = repository.getTransformationID(transName, repositoryDirectory);
		if (transformationID == null) {
			return null;
		}
		TransMeta transMeta = repository.loadTransformation(transformationID, null);
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobName
	 * @return
	 * @throws KettleException
	 */
	public JobMeta getJobMeta(String jobName) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		ObjectId jobID = repository.getJobId(jobName, repositoryDirectory);
		if (jobID == null) {
			return null;
		}
		JobMeta jobMeta = repository.loadJob(jobID, null);
		return jobMeta;
	}

	/**
	 * 从资源库获取TransMeta
	 *
	 * @param name
	 * @return
	 * @throws KettleException
	 */
	public TransMeta getTransMeta(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		TransMeta transMeta = repository.loadTransformation(new LongObjectId(transID), null);
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobID
	 * @return
	 * @throws KettleException
	 */
	public JobMeta getJobMeta(long jobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		JobMeta jobMeta = repository.loadJob(new LongObjectId(jobID), null);
		return jobMeta;
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void saveTransMeta(TransMeta transMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		transMeta.setRepositoryDirectory(repositoryDirectory);
		repository.save(transMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void saveJobMeta(JobMeta jobMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		jobMeta.setRepositoryDirectory(repositoryDirectory);
		repository.save(jobMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public void deleteTransMeta(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.deleteTransformation(new LongObjectId(transID));
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public void deleteJobMeta(long jobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.deleteJob(new LongObjectId(jobID));
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public void deleteTransMetaForce(long transID) throws KettleException {
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
	public KettleJobRecord queryJobRecord(long jobID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = repository.connectionDelegate.getOneRow(KettleVariables.R_JOB_RECORD,
				KettleVariables.R_JOB_RECORD_ID_JOB, new LongObjectId(jobID));
		if (table == null || table.size() < 1) {
			return null;
		} else {
			KettleJobRecord job = new KettleJobRecord();
			job.setId(jobID);
			job.setName(table.getString(KettleVariables.R_JOB_RECORD_NAME_JOB, null));
			job.setRunID(table.getString(KettleVariables.R_RECORD_ID_RUN, null));
			job.setStatus(table.getString(KettleVariables.R_RECORD_STATUS, null));
			job.setHostname(table.getString(KettleVariables.R_RECORD_HOSTNAME, null));
			job.setErrMsg(table.getString(KettleVariables.R_RECORD_ERRORMSG, null));
			job.setKettleMeta(getJobMeta(jobID));
			return job;
		}
	}

	/**
	 * 持久化操作:查询转换记录
	 * 
	 * @throws KettleException
	 * 
	 * @throws KettleException
	 */
	public KettleTransRecord queryTransRecord(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = repository.connectionDelegate.getOneRow(KettleVariables.R_TRANS_RECORD,
				KettleVariables.R_TRANS_RECORD_ID_TRANS, new LongObjectId(transID));
		if (table == null || table.size() < 1) {
			return null;
		} else {
			KettleTransRecord trans = new KettleTransRecord();
			trans.setId(transID);
			trans.setName(table.getString(KettleVariables.R_TRANS_RECORD_NAME_TRANS, null));
			trans.setRunID(table.getString(KettleVariables.R_RECORD_ID_RUN, null));
			trans.setStatus(table.getString(KettleVariables.R_RECORD_STATUS, null));
			trans.setHostname(table.getString(KettleVariables.R_RECORD_HOSTNAME, null));
			trans.setErrMsg(table.getString(KettleVariables.R_RECORD_ERRORMSG, null));
			trans.setKettleMeta(getTransMeta(transID));
			return trans;
		}
	}

	/**
	 * 持久化操作:Insert转换记录
	 * 
	 * 
	 * @throws KettleException
	 */
	public synchronized void insertTransRecord(KettleTransRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_TRANS_RECORD_ID_TRANS, ValueMetaInterface.TYPE_INTEGER),
				record.getId());
		table.addValue(new ValueMeta(KettleVariables.R_TRANS_RECORD_NAME_TRANS, ValueMetaInterface.TYPE_STRING),
				record.getName());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				record.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		repository.connectionDelegate.insertTableRow(KettleVariables.R_TRANS_RECORD, table);
		repository.commit();
	}

	/**
	 * 持久化操作:Insert工作记录
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void insertJobRecord(KettleJobRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
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
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_RECORD_TYPE, ValueMetaInterface.TYPE_STRING),
					record.getRecordType());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
					record.getRunID());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					record.getStatus());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
					record.getHostname());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
					record.getErrMsg());
			repository.connectionDelegate.insertTableRow(KettleVariables.R_HISTORY_RECORD, table);
		}
	}

	/**
	 * 更新工作
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void updateJobRecord(KettleJobRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				record.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_TIMESTAMP), null);
		repository.connectionDelegate.updateTableRow(KettleVariables.R_JOB_RECORD, KettleVariables.R_JOB_RECORD_ID_JOB,
				table, new LongObjectId(record.getId()));
		insertHistory(record);
		repository.commit();
	}

	/**
	 * 持久化操作:批量更新记录
	 * 
	 * @throws KettleException
	 */
	public synchronized void updateTransRecord(KettleTransRecord record) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				record.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_TIMESTAMP), null);
		if (KettleTransRecord.class.isInstance(record)) {
			repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD,
					KettleVariables.R_TRANS_RECORD_ID_TRANS, table, new LongObjectId(record.getId()));
		}
		if (KettleJobRecord.class.isInstance(record)) {
			repository.connectionDelegate.updateTableRow(KettleVariables.R_JOB_RECORD,
					KettleVariables.R_JOB_RECORD_ID_JOB, table, new LongObjectId(record.getId()));
		}
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
			table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					record.getStatus());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
					record.getRunID());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
					record.getHostname());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
					record.getErrMsg());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_TIMESTAMP), null);
			repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD,
					KettleVariables.R_TRANS_RECORD_ID_TRANS, table, new LongObjectId(record.getId()));
			insertHistory(record);
		}
		repository.commit();
	}

	/**
	 * 持久化操作:删除转换记录
	 * 
	 * @throws KettleException
	 */
	public void deleteTransRecord(long transID) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		repository.connectionDelegate.performDelete("DELETE FROM " + KettleVariables.R_TRANS_RECORD + " WHERE "
				+ KettleVariables.R_TRANS_RECORD_ID_TRANS + " = ?", new LongObjectId(transID));
		repository.commit();
	}

	/**
	 * 获取所有需处理Trans
	 * 
	 * @param hostname
	 * @return
	 * @throws KettleException
	 */
	public List<KettleTransRecord> allHandleTransRecord(List<String> hostnames) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		StringBuffer hostNamesSql = new StringBuffer();
		for (String hostName : hostnames) {
			if (hostNamesSql.length() > 0) {
				hostNamesSql.append(", '").append(hostName).append("'");
			} else {
				hostNamesSql.append("'").append(hostName).append("'");
			}
		}
		String sql = "SELECT " + KettleVariables.R_TRANS_RECORD_ID_TRANS + ","
				+ KettleVariables.R_TRANS_RECORD_NAME_TRANS + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_ERRORMSG + " FROM "
				+ KettleVariables.R_TRANS_RECORD + " WHERE " + KettleVariables.R_RECORD_HOSTNAME + " in ("
				+ hostNamesSql.toString() + ") AND " + KettleVariables.R_RECORD_STATUS + " in ('"
				+ KettleVariables.RECORD_STATUS_RUNNING + "', '" + KettleVariables.RECORD_STATUS_APPLY + "')";
		List<Object[]> result = repository.connectionDelegate.getRows(sql, -1);
		List<KettleTransRecord> kettleTransBeans = new LinkedList<KettleTransRecord>();
		if (result == null || result.isEmpty()) {
			return kettleTransBeans;
		}
		KettleTransRecord bean = null;
		for (Object[] record : result) {
			bean = new KettleTransRecord();
			bean.setId((Long) record[0]);
			bean.setName((String) record[1]);
			bean.setRunID((String) record[2]);
			bean.setStatus((String) record[3]);
			bean.setHostname((String) record[4]);
			bean.setCreateTime((Timestamp) record[5]);
			bean.setErrMsg((String) record[6]);
			bean.setKettleMeta(getTransMeta(Long.valueOf(bean.getId())));
			kettleTransBeans.add(bean);
		}
		return kettleTransBeans;
	}

	/**
	 * 获取所有需处理Job任务
	 * 
	 * @param hostname
	 * @return
	 * @throws KettleException
	 */
	public List<KettleJobRecord> allHandleJobRecord(List<String> hostnames) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		StringBuffer hostNamesSql = new StringBuffer();
		for (String hostName : hostnames) {
			if (hostNamesSql.length() > 0) {
				hostNamesSql.append(", '").append(hostName).append("'");
			} else {
				hostNamesSql.append("'").append(hostName).append("'");
			}
		}
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_ID_JOB + "," + KettleVariables.R_JOB_RECORD_NAME_JOB + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + "," + KettleVariables.R_RECORD_CREATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + " FROM " + KettleVariables.R_JOB_RECORD + " WHERE "
				+ KettleVariables.R_RECORD_HOSTNAME + " in (" + hostNamesSql.toString() + ") AND "
				+ KettleVariables.R_RECORD_STATUS + " in ('" + KettleVariables.RECORD_STATUS_RUNNING + "', '"
				+ KettleVariables.RECORD_STATUS_APPLY + "')";
		List<Object[]> result = repository.connectionDelegate.getRows(sql, -1);
		List<KettleJobRecord> kettleJobBeans = new LinkedList<KettleJobRecord>();
		if (result == null || result.isEmpty()) {
			return kettleJobBeans;
		}
		KettleJobRecord bean = null;
		for (Object[] record : result) {
			bean = new KettleJobRecord();
			bean.setId((Long) record[0]);
			bean.setName((String) record[1]);
			bean.setRunID((String) record[2]);
			bean.setStatus((String) record[3]);
			bean.setHostname((String) record[4]);
			bean.setCreateTime((Timestamp) record[5]);
			bean.setErrMsg((String) record[6]);
			bean.setKettleMeta(getJobMeta(Long.valueOf(bean.getId())));
			kettleJobBeans.add(bean);
		}
		return kettleJobBeans;
	}

	public KettleDatabaseRepository getRepository() {
		return repository;
	}

}
