package com.kettle.core.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleRecord;
import com.kettle.core.bean.KettleRecordRelation;

/**
 * Kettle的数据库Client,记录KettleRecord对象
 * @author Administrator
 *
 */
public class KettleDBClient {

	/**
	 * 日志
	 */
	Logger logger = LoggerFactory.getLogger(KettleDBClient.class);
	/**
	 * 数据库元数据
	 */
	private final Database database;

	public KettleDBClient(DatabaseMeta databaseMeta) throws KettleDatabaseException {
		database = new Database(new SimpleLoggingObject("Kettel DB", LoggingObjectType.DATABASE, null), databaseMeta);
	}

	/**
	 * 数据库连接
	 */
	private synchronized void connect() {
		try {
			database.initializeVariablesFrom(null);
			database.connect();
			database.setAutoCommit(false);
		} catch (KettleException e) {
			e.printStackTrace();
			throw new RuntimeException("Kettle的数据库无法连接!");
		}
	}

	/**
	 * 数据库断开
	 */
	private synchronized void disConnect() {
		try {
			database.commit();
		} catch (KettleDatabaseException e) {
			e.printStackTrace();
			throw new RuntimeException("Kettle的数据库提交失败!");
		}
		database.disconnect();
	}

	/**
	 * 查询一条数据
	 * 
	 * @param sql
	 * @param type
	 * @param id
	 * @return
	 * @throws KettleException
	 * @throws KettleDatabaseException
	 */
	private synchronized RowMetaAndData queryOneRow(String sql, int type, Object id) throws KettleException {
		ResultSet resultSet = null;
		try {
			PreparedStatement ps = database.prepareSQL(sql);
			RowMetaInterface parameterMeta = new RowMeta();
			parameterMeta.addValueMeta(new ValueMeta("id", ValueMetaInterface.TYPE_STRING));
			Object[] parameterData = new Object[] { id, };
			resultSet = database.openQuery(ps, parameterMeta, parameterData);
			Object[] result = database.getRow(resultSet);
			if (result == null) {
				return new RowMetaAndData(database.getReturnRowMeta(),
						RowDataUtil.allocateRowData(database.getReturnRowMeta().size()));
			}
			return new RowMetaAndData(database.getReturnRowMeta(), result);
		} catch (KettleDatabaseException e) {
			throw new KettleException("KettleDB查询发生异常!", e);
		} finally {
			if (resultSet != null) {
				database.closeQuery(resultSet);
			}
		}

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
	private synchronized void deleteTableRow(String sql, int type, Object id) throws KettleException {
		try {
			PreparedStatement ps = database.prepareSQL(sql);
			RowMetaInterface parameterMeta = new RowMeta();
			parameterMeta.addValueMeta(new ValueMeta("id1", type));
			Object[] parameterData = new Object[1];
			parameterData[0] = id;
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
	public synchronized KettleRecord queryRecord(String uuid) throws KettleException {
		connect();
		RowMetaAndData table;
		ResultSet resultSet = null;
		try {
			String sql = "SELECT * FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_UUID
					+ " = ?";
			table = queryOneRow(sql, ValueMetaInterface.TYPE_STRING, uuid);
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
		job.setUuid(table.getString(KettleVariables.R_JOB_RECORD_UUID, null));
		job.setJobid(table.getString(KettleVariables.R_JOB_RECORD_ID_JOB, null));
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
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_UUID, ValueMetaInterface.TYPE_STRING),
				record.getUuid());
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_ID_JOB, ValueMetaInterface.TYPE_STRING),
				record.getJobid());
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
			table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_UUID, ValueMetaInterface.TYPE_STRING),
					record.getUuid());
			table.addValue(new ValueMeta(KettleVariables.R_HISTORY_RECORD_ID, ValueMetaInterface.TYPE_STRING),
					record.getJobid());
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
			insertTableRow(KettleVariables.R_HISTORY_RECORD, table);
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
			updateTableRow(KettleVariables.R_JOB_RECORD, table, KettleVariables.R_JOB_RECORD_UUID,
					ValueMetaInterface.TYPE_STRING, record.getUuid());
			insertHistory(record);
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
	public synchronized List<KettleRecordRelation> queryDependents(String mainJobUUID) throws KettleException {
		String sql = "SELECT " + KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID + ","
				+ KettleVariables.R_RECORD_DEPENDENT_META_ID + "," + KettleVariables.R_RECORD_DEPENDENT_META_TYPE
				+ " FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID + " = '" + mainJobUUID + "'";
		List<Object[]> result = null;
		List<KettleRecordRelation> depends = new LinkedList<KettleRecordRelation>();
		connect();
		try {
			result = database.getRows(sql, -1);
		} finally {
			disConnect();
		}
		if (result == null) {
			return depends;
		}
		KettleRecordRelation depend = null;
		for (Object[] row : result) {
			depend = new KettleRecordRelation();
			depend.setMasterUUID((String) row[0]);
			depend.setMetaid((String) row[1]);
			depend.setType((String) row[2]);
			depend.setCreateTime((Date) row[3]);
			depends.add(depend);
		}
		return depends;
	}

	/**
	 * 维护关系
	 * 
	 * @param dependentTrans
	 * @param dependentJobs
	 * @param mainJob
	 * @param recordUUID
	 * @throws KettleException
	 */
	public synchronized void saveDependentsRelation(KettleJobEntireDefine jobEntire) throws KettleException {
		Date now = new Date();
		RowMetaAndData data = null;
		connect();
		try {
			for (TransMeta meta : jobEntire.getDependentTrans()) {
				data = new RowMetaAndData();
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID,
						ValueMetaInterface.TYPE_STRING), jobEntire.getUuid());
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_STRING),
						meta.getObjectId().getId());
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
						KettleVariables.RECORD_TYPE_TRANS);
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
				insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
			}
			for (JobMeta meta : jobEntire.getDependentJobs()) {
				data = new RowMetaAndData();
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID,
						ValueMetaInterface.TYPE_STRING), jobEntire.getUuid());
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_STRING),
						meta.getObjectId().getId());
				data.addValue(
						new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
						KettleVariables.RECORD_TYPE_JOB);
				data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
				insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
			}
			// mainJob
			data = new RowMetaAndData();
			data.addValue(
					new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID, ValueMetaInterface.TYPE_STRING),
					jobEntire.getUuid());
			data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_STRING),
					jobEntire.getMainJob().getObjectId().getId());
			data.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
					KettleVariables.RECORD_TYPE_JOB);
			data.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
			insertTableRow(KettleVariables.R_RECORD_DEPENDENT, data);
		} finally {
			disConnect();
		}
	}

	/**
	 * @param uuid
	 */
	public List<KettleRecordRelation> deleteDependentsRelationNE(String uuid) {
		String sql = "DELETE FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID + " = ? ";
		connect();
		try {
			List<KettleRecordRelation> depends = this.queryDependents(uuid);
			deleteTableRow(sql, ValueMetaInterface.TYPE_STRING, uuid);
			return depends;
		} catch (KettleException e) {
			logger.error("数据库删除record[" + uuid + "]的依赖表发生异常!", e);
			return new ArrayList<KettleRecordRelation>(0);
		} finally {
			disConnect();
		}

	}

	/**
	 * 删除JOB
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteRecord(String uuid) throws KettleException {
		String sql = "DELETE FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_UUID
				+ " = ? ";
		connect();
		try {
			deleteTableRow(sql, ValueMetaInterface.TYPE_STRING, uuid);
		} finally {
			disConnect();
		}
	}

	/**
	 * 删除JOB
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteRecordNE(String uuid) {
		try {
			deleteRecord(uuid);
		} catch (Exception ex) {
			logger.error("数据库删除record[" + uuid + "]发生异常!", ex);
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
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
				+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
				+ KettleVariables.R_JOB_RECORD + " WHERE (" + KettleVariables.R_RECORD_CRON_EXPRESSION
				+ " IS NOT NULL OR " + KettleVariables.R_RECORD_STATUS + " in ('"
				+ KettleVariables.RECORD_STATUS_RUNNING + "', '" + KettleVariables.RECORD_STATUS_APPLY + "'))";
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
			bean.setUuid((String) record[0]);
			bean.setJobid((String) record[1]);
			bean.setName((String) record[2]);
			bean.setRunID((String) record[3]);
			bean.setStatus((String) record[4]);
			bean.setHostname(record[5] == null ? null : (String) record[5]);
			bean.setCreateTime((Date) record[6]);
			bean.setUpdateTime((Date) record[7]);
			bean.setErrMsg(record[8] == null ? null : (String) record[8]);
			bean.setCronExpression(record[9] == null ? null : (String) record[9]);
			kettleJobBeans.add(bean);
		}
		return kettleJobBeans;
	}

	/**
	 * 获取所有停止了的Record
	 * 
	 * @return
	 * @throws KettleDatabaseException
	 */
	public synchronized List<KettleRecord> allStopRecord() throws KettleDatabaseException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
				+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
				+ KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NULL AND "
				+ KettleVariables.R_RECORD_STATUS + " in ('" + KettleVariables.RECORD_STATUS_FINISHED + "', '"
				+ KettleVariables.RECORD_STATUS_ERROR + "');";
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
			bean.setUuid((String) record[0]);
			bean.setJobid((String) record[1]);
			bean.setName((String) record[2]);
			bean.setRunID((String) record[3]);
			bean.setStatus((String) record[4]);
			bean.setHostname(record[5] == null ? null : (String) record[5]);
			bean.setCreateTime((Date) record[6]);
			bean.setUpdateTime((Date) record[7]);
			bean.setErrMsg(record[8] == null ? null : (String) record[8]);
			bean.setCronExpression(record[9] == null ? null : (String) record[9]);
			kettleJobBeans.add(bean);
		}
		return kettleJobBeans;
	}
}
