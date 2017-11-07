package com.kettle.core.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordDepend;

public class KettleDBClient {
	/**
	 * 数据库元数据
	 */
	private final Database database;

	public KettleDBClient(DatabaseMeta databaseMeta) {
		database = new Database(new SimpleLoggingObject("Kettel DB", LoggingObjectType.DATABASE, null), databaseMeta);
	}

	/**
	 * 数据库连接
	 */
	private synchronized void connect() {
		try {
			database.initializeVariablesFrom(null);
			database.connect();
		} catch (KettleException e) {
			throw new RuntimeException("Kettle的数据库无法连接!");
		}
	}

	/**
	 * 数据库断开
	 */
	private synchronized void disConnect() {
		database.disconnect();
	}

	/**
	 * 查询一条数据
	 * 
	 * @param sql
	 * @param type
	 * @param id
	 * @return
	 * @throws KettleDatabaseException
	 */
	private RowMetaAndData queryOneRow(String sql, int type, Object id) throws KettleDatabaseException {
		RowMetaInterface parameterMeta = new RowMeta();
		parameterMeta.addValueMeta(new ValueMeta("id", ValueMetaInterface.TYPE_INTEGER));
		Object[] parameterData = new Object[] { id };
		return database.getOneRow(sql, parameterMeta, parameterData);
	}

	/**
	 * 保存表
	 * 
	 * @param tablename
	 * @param values
	 * @throws KettleException
	 */
	private synchronized void insertTableRow(String tablename, RowMetaAndData values) throws KettleException {
		database.prepareInsert(values.getRowMeta(), tablename);
		database.setValuesInsert(values);
		database.insertRow();
		database.closeInsert();
	}

	/**
	 * 更新表
	 * 
	 * @param tablename
	 * @param values
	 * @param idfield
	 * @param type
	 * @param id
	 * @throws KettleException
	 */
	private synchronized void updateTableRow(String tablename, RowMetaAndData values, String idfield, int type,
			Object id) throws KettleException {
		String[] sets = new String[values.size()];
		for (int i = 0; i < values.size(); i++) {
			sets[i] = values.getValueMeta(i).getName();
		}
		String[] codes = new String[] { idfield };
		String[] condition = new String[] { "=" };
		database.prepareUpdate(tablename, codes, condition, sets);
		values.addValue(new ValueMeta(idfield, type), id);
		database.setValuesUpdate(values.getRowMeta(), values.getData());
		database.updateRow();
		database.closeUpdate();
	}

	/**
	 * 删除一条数据
	 * 
	 * @param sql
	 * @param type
	 * @param id
	 * @throws KettleException
	 */
	private void deleteTableRow(String sql, int type, Object id) throws KettleException {
		try {
			PreparedStatement ps = database.prepareSQL(sql);
			RowMetaInterface parameterMeta = new RowMeta();
			parameterMeta.addValueMeta(new ValueMeta("id1", type));
			Object[] parameterData = new Object[1];
			parameterData[1] = id;
			RowMetaAndData param = new RowMetaAndData(parameterMeta, parameterData);
			database.setValues(param, ps);
			ps.execute();
		} catch (SQLException e) {
			throw new KettleException("Unable to perform delete with SQL: " + sql + ", id=" + id, e);
		}
	}

	/**
	 * 查询Record记录
	 * 
	 * @throws KettleException
	 */
	public KettleRecord queryRecord(long id) throws KettleException {
		connect();
		RowMetaAndData table;
		ResultSet resultSet = null;
		try {
			String sql = "SELECT * FROM " + KettleVariables.R_JOB_RECORD + " WHERE "
					+ KettleVariables.R_JOB_RECORD_ID_JOB + " = ?";
			table = queryOneRow(sql, ValueMetaInterface.TYPE_INTEGER, id);
		} finally {
			if (resultSet != null) {
				database.closeQuery(resultSet);
			}
			disConnect();
		}
		KettleRecord job = new KettleRecord();
		if (table == null || table.size() < 1) {
			return null;
		}
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

	/**
	 * 持久化操作:Insert工作记录
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void insertRecord(KettleRecord record) throws KettleException {
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
		connect();
		try {
			insertTableRow(KettleVariables.R_JOB_RECORD, table);
		} finally {
			disConnect();
		}
	}

	/**
	 * 保存历史表
	 * 
	 * @param record
	 * @throws KettleException
	 */
	private synchronized void insertHistory(KettleRecord record) throws KettleException {
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
			connect();
			try {
				insertTableRow(KettleVariables.R_HISTORY_RECORD, table);
			} finally {
				disConnect();
			}
		}
	}

	/**
	 * 更新工作
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public synchronized void updateRecord(KettleRecord record) throws KettleException {
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
		connect();
		try {
			String[] sets = new String[table.size()];
			for (int i = 0; i < table.size(); i++) {
				sets[i] = table.getValueMeta(i).getName();
			}
			updateTableRow(KettleVariables.R_JOB_RECORD, table, KettleVariables.R_JOB_RECORD_ID_JOB,
					ValueMetaInterface.TYPE_INTEGER, record.getId());
			insertHistory(record);
			if (!database.isAutoCommit()) {
				database.commit();
			}
		} finally {
			disConnect();
		}
	}

	/**
	 * 持查询依赖
	 * 
	 * @param mainJobID
	 * @throws KettleException
	 */
	public synchronized List<KettleRecordDepend> queryDependents(long mainJobID) throws KettleException {
		String sql = "SELECT " + KettleVariables.R_RECORD_DEPENDENT_MASTER_ID + ","
				+ KettleVariables.R_RECORD_DEPENDENT_META_ID + "," + KettleVariables.R_RECORD_DEPENDENT_META_TYPE
				+ " FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_ID + " = " + mainJobID;
		List<Object[]> result = null;
		List<KettleRecordDepend> depends = new LinkedList<KettleRecordDepend>();
		connect();
		try {
			result = database.getRows(sql, -1);
		} finally {
			disConnect();
		}
		if (result == null) {
			return depends;
		}
		KettleRecordDepend depend = null;
		for (Object[] row : result) {
			depend = new KettleRecordDepend();
			depend.setId((Long) row[1]);
			depend.setType((String) row[2]);
			depend.setCreateTime((Date) row[3]);
			depends.add(depend);
		}
		return depends;
	}

	/**
	 * 删除JOB
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteRecord(long jobID) throws KettleException {
		String sql = "DELETE FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_ID_JOB
				+ " = ? ";
		connect();
		try {
			deleteTableRow(sql, ValueMetaInterface.TYPE_INTEGER, jobID);
			database.commit();
		} finally {
			disConnect();
		}
	}

	/**
	 * 获取所有需处理Job任务
	 * 
	 * @param hostname
	 * @return
	 * @throws KettleException
	 */
	public synchronized List<KettleRecord> allHandleRecord() throws KettleException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_ID_JOB + "," + KettleVariables.R_JOB_RECORD_NAME_JOB + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + "," + KettleVariables.R_RECORD_CREATETIME + ","
				+ KettleVariables.R_RECORD_UPDATETIME + "," + KettleVariables.R_RECORD_ERRORMSG + ","
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM " + KettleVariables.R_JOB_RECORD + " WHERE ("
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NOT NULL OR " + KettleVariables.R_RECORD_STATUS
				+ " in ('" + KettleVariables.RECORD_STATUS_RUNNING + "', '" + KettleVariables.RECORD_STATUS_APPLY
				+ "'))";
		List<Object[]> result = null;
		connect();
		try {
			result = database.getRows(sql, -1);
		} finally {
			disConnect();
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
		Date now = new Date();
		RowMetaAndData data = null;
		connect();
		try {
			if (dependentTrans != null && !dependentTrans.isEmpty()) {
				for (TransMeta meta : dependentTrans) {
					data = new RowMetaAndData();
					data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_ID,
							ValueMetaInterface.TYPE_INTEGER), Long.valueOf(mainJob.getObjectId().getId()));
					data.addValue(
							new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_INTEGER),
							Long.valueOf(meta.getObjectId().getId()));
					data.addValue(
							new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
							KettleVariables.RECORD_TYPE_TRANS);
					data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE),
							now);
					insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
				}
			}
			if (dependentJobs != null && !dependentJobs.isEmpty()) {
				for (JobMeta meta : dependentJobs) {
					data = new RowMetaAndData();
					data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_ID,
							ValueMetaInterface.TYPE_INTEGER), Long.valueOf(mainJob.getObjectId().getId()));
					data.addValue(
							new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_INTEGER),
							Long.valueOf(meta.getObjectId().getId()));
					data.addValue(
							new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
							KettleVariables.RECORD_TYPE_JOB);
					data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE),
							now);
					insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
				}
			}
		} finally {
			disConnect();
		}
	}

	/**
	 * 获取所有停止了的Record
	 * 
	 * @return
	 * @throws KettleDatabaseException
	 */
	public synchronized List<KettleRecord> allStopRecord() throws KettleDatabaseException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_ID_JOB + "," + KettleVariables.R_JOB_RECORD_NAME_JOB + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + "," + KettleVariables.R_RECORD_CREATETIME + ","
				+ KettleVariables.R_RECORD_UPDATETIME + "," + KettleVariables.R_RECORD_ERRORMSG + ","
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM " + KettleVariables.R_JOB_RECORD + " WHERE "
				+ KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NULL AND " + KettleVariables.R_RECORD_STATUS
				+ " in ('" + KettleVariables.RECORD_STATUS_FINISHED + "', '" + KettleVariables.RECORD_STATUS_ERROR
				+ "');";
		List<Object[]> result = null;
		connect();
		try {
			result = database.getRows(sql, -1);
		} finally {
			disConnect();
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
