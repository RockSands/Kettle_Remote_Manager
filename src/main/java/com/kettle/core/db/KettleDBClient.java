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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordRelation;

/**
 * Kettle的数据库Client,记录KettleRecord对象
 * 
 * @author Administrator
 *
 */
public class KettleDBClient {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleDBClient.class);

	/**
	 * 数据库元数据
	 */
	private final Database database;

	/**
	 * @param databaseMeta
	 * @throws KettleDatabaseException
	 */
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
			database.setAutoCommit(true);
		} catch (KettleException e) {
			e.printStackTrace();
			throw new RuntimeException("Kettle的数据库无法连接!");
		}
	}

	/**
	 * 数据库断开
	 */
	private synchronized void closeConnect() {
		try {
			database.closeConnectionOnly();
		} catch (KettleDatabaseException e) {
			e.printStackTrace();
			throw new RuntimeException("Kettle的数据库关闭失败!");
		}
	}

	/**
	 * 查询一条数据
	 * 
	 * @param sql
	 * @param type
	 * @param id
	 * @return
	 * @throws KettleException
	 */
	private synchronized RowMetaAndData queryOneRow(String sql, int type, Object id) throws KettleException {
		ResultSet resultSet = null;
		connect();
		try {
			PreparedStatement ps = database.prepareSQL(sql);
			RowMetaInterface parameterMeta = new RowMeta();
			parameterMeta.addValueMeta(new ValueMeta("id", type));
			Object[] parameterData = new Object[] { id, };
			resultSet = database.openQuery(ps, parameterMeta, parameterData);
			Object[] result = database.getRow(resultSet);
			if (result == null) {
				return new RowMetaAndData(database.getReturnRowMeta(),
						RowDataUtil.allocateRowData(database.getReturnRowMeta().size()));
			}
			return new RowMetaAndData(database.getReturnRowMeta(), result);
		} finally {
			if (resultSet != null) {
				try {
					database.closeQuery(resultSet);
				} catch (Exception ex) {
				}
			}
			closeConnect();
		}
	}

	/**
	 * 查询一条数据
	 * 
	 * @param sql
	 * @param type
	 * @param id
	 * @return
	 * @throws KettleException
	 */
	private synchronized List<RowMetaAndData> queryRows(String sql, int type, Object id) throws KettleException {
		ResultSet resultSet = null;
		connect();
		try {
			PreparedStatement ps = database.prepareSQL(sql);
			RowMetaInterface parameterMeta = new RowMeta();
			parameterMeta.addValueMeta(new ValueMeta("id", type));
			Object[] parameterData = new Object[] { id, };
			resultSet = database.openQuery(ps, parameterMeta, parameterData);
			List<Object[]> results = database.getRows(resultSet, -1, null);
			List<RowMetaAndData> rowMetaAndDatas = new ArrayList<RowMetaAndData>(results == null ? 1 : results.size());
			if (results == null) {
				rowMetaAndDatas.add(new RowMetaAndData(database.getReturnRowMeta(),
						RowDataUtil.allocateRowData(database.getReturnRowMeta().size())));
			}
			for (Object[] result : results) {
				rowMetaAndDatas.add(new RowMetaAndData(database.getReturnRowMeta(), result));
			}
			return rowMetaAndDatas;
		} finally {
			if (resultSet != null) {
				try {
					database.closeQuery(resultSet);
				} catch (Exception ex) {
				}
			}
			closeConnect();
		}
	}

	/**
	 * 查询多条数据
	 * 
	 * @param sql
	 * @return
	 * @throws KettleException
	 */
	private synchronized List<Object[]> queryRows(String sql) throws KettleException {
		connect();
		try {
			return database.getRows(sql, -1);
		} finally {
			closeConnect();
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
		connect();
		try {
			database.prepareInsert(values.getRowMeta(), tablename);
			database.setValuesInsert(values);
			database.insertRow();
			database.closeInsert();
		} finally {
			closeConnect();
		}
	}

	/**
	 * 保存表
	 * 
	 * @param tablename
	 * @param values
	 * @throws KettleException
	 */
	private synchronized void insertTableRows(String tablename, List<RowMetaAndData> valuesList)
			throws KettleException {
		connect();
		try {
			RowMetaInterface rowMeta = valuesList.get(0).getRowMeta();
			database.prepareInsert(rowMeta, tablename);
			for (RowMetaAndData rowMetaAndData : valuesList) {
				database.setValuesInsert(rowMetaAndData);
				database.insertRow();
			}
			database.closeInsert();
		} finally {
			closeConnect();
		}
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
	private synchronized void updateTableRow(String tablename, RowMetaAndData values, String idfield)
			throws KettleException {
		String[] sets = new String[values.size()];
		for (int i = 0; i < values.size(); i++) {
			sets[i] = values.getValueMeta(i).getName();
		}
		String[] codes = new String[] { idfield };
		String[] condition = new String[] { "=" };
		connect();
		try {
			database.prepareUpdate(tablename, codes, condition, sets);
			database.setValuesUpdate(values.getRowMeta(), values.getData());
			database.updateRow();
			database.closeUpdate();
		} finally {
			closeConnect();
		}
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
	private synchronized void updateTableRows(String tablename, List<RowMetaAndData> valuesList, String idfield)
			throws KettleException {
		List<String[]> setsList = new ArrayList<String[]>(valuesList.size());
		String[] set = null;
		for (RowMetaAndData values : valuesList) {
			set = new String[values.size()];
			setsList.add(set);
			for (int i = 0; i < values.size(); i++) {
				set[i] = values.getValueMeta(i).getName();
			}
		}
		String[] codes = new String[] { idfield };
		String[] condition = new String[] { "=" };
		connect();
		try {
			for (int i = 0, size = valuesList.size(); i < size; i++) {
				database.prepareUpdate(tablename, codes, condition, setsList.get(i));
				database.setValuesUpdate(valuesList.get(i).getRowMeta(), valuesList.get(i).getData());
				database.updateRow();
			}
			database.closeUpdate();
		} finally {
			closeConnect();
		}
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
		connect();
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
		} finally {
			closeConnect();
		}
	}

	/**
	 * 查询Record记录
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public KettleRecord queryRecord(String uuid) throws KettleException {
		RowMetaAndData table;
		String sql = "SELECT * FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_UUID
				+ " = ?";
		table = queryOneRow(sql, ValueMetaInterface.TYPE_STRING, uuid);
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
	 * 查询Record记录
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public KettleRecord queryRecordRelations(KettleRecord record) throws KettleException {
		String sql = "SELECT * FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID + " = '" + record.getUuid() + "'";
		List<RowMetaAndData> relations = queryRows(sql, ValueMetaInterface.TYPE_STRING, record.getUuid());
		KettleRecordRelation bean;
		for (RowMetaAndData relation : relations) {
			bean = new KettleRecordRelation();
			bean.setCreateTime(relation.getDate(KettleVariables.R_RECORD_CREATETIME, null));
			bean.setMasterUUID(relation.getString(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID, null));
			bean.setMetaid(relation.getString(KettleVariables.R_RECORD_DEPENDENT_META_ID, null));
			bean.setType(relation.getString(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, null));
			record.getRelations().add(bean);
		}
		return record;
	}

	/**
	 * 查询Record记录
	 * 
	 * @param uuids
	 * @throws KettleException
	 */
	public List<KettleRecord> queryRecords(List<String> uuids) throws KettleException {
		String[] inStrArr = new String[(uuids.size() / 128) + 1];
		StringBuffer strBuffer = new StringBuffer();
		for (int i = 0; i < uuids.size(); i++) {
			strBuffer.append(",'").append(uuids.get(i)).append("'");
			if (i % 128 == 127 || i == uuids.size() - 1) {
				inStrArr[(i / 128)] = strBuffer.substring(1);
				strBuffer.delete(0, strBuffer.length());
			}
		}
		String sql;
		List<Object[]> result = null;
		KettleRecord bean = null;
		List<KettleRecord> kettleRecords = new LinkedList<KettleRecord>();
		for (String inStr : inStrArr) {
			sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
					+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
					+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
					+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
					+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
					+ KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_RECORD_CRON_EXPRESSION
					+ " IS NULL AND " + KettleVariables.R_JOB_RECORD_UUID + " in (" + inStr + ");";

			result = queryRows(sql);
			if (result == null || result.isEmpty()) {
				continue;
			}
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
				kettleRecords.add(bean);
			}
		}
		return kettleRecords;
	}

	/**
	 * 持久化操作:Insert工作记录
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void insertRecord(KettleRecord record) throws KettleException {
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
		insertTableRow(KettleVariables.R_JOB_RECORD, table);
		/*
		 * 保存依赖
		 */
		List<RowMetaAndData> allTables = new ArrayList<RowMetaAndData>(record.getRelations().size());
		for (KettleRecordRelation relation : record.getRelations()) {
			table = new RowMetaAndData();
			table.addValue(
					new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID, ValueMetaInterface.TYPE_STRING),
					relation.getMasterUUID());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_STRING),
					relation.getMetaid());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
					relation.getType());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
			allTables.add(table);
		}
		insertTableRows(KettleVariables.R_RECORD_DEPENDENT, allTables);
	}

	/**
	 * 保存历史表
	 * 
	 * @param record
	 * @throws KettleException
	 */
	private void insertHistory(KettleRecord record) throws KettleException {
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
	public void updateRecordNoStatus(KettleRecord record) throws KettleException {
		RowMetaAndData table = new RowMetaAndData();
		record.setUpdateTime(new Date());
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_ID_JOB, ValueMetaInterface.TYPE_STRING),
				record.getJobid());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				record.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				record.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ERRORMSG, ValueMetaInterface.TYPE_STRING),
				record.getErrMsg());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_UPDATETIME, ValueMetaInterface.TYPE_DATE),
				record.getUpdateTime());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_CRON_EXPRESSION, ValueMetaInterface.TYPE_STRING),
				record.getCronExpression());
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_UUID, ValueMetaInterface.TYPE_STRING),
				record.getUuid());
		String[] sets = new String[table.size()];
		for (int i = 0; i < table.size(); i++) {
			sets[i] = table.getValueMeta(i).getName();
		}
		updateTableRow(KettleVariables.R_JOB_RECORD, table, KettleVariables.R_JOB_RECORD_UUID);
		insertHistory(record);
	}

	/**
	 * 更新工作
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void updateRecordStatus(KettleRecord record) throws KettleException {
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
		table.addValue(new ValueMeta(KettleVariables.R_JOB_RECORD_UUID, ValueMetaInterface.TYPE_STRING),
				record.getUuid());
		String[] sets = new String[table.size()];
		for (int i = 0; i < table.size(); i++) {
			sets[i] = table.getValueMeta(i).getName();
			updateTableRow(KettleVariables.R_JOB_RECORD, table, KettleVariables.R_JOB_RECORD_UUID);
		}
		insertHistory(record);
	}

	/**
	 * 更新Record
	 * 
	 * @param record
	 */
	public void updateRecordStatusNE(KettleRecord record) {
		try {
			updateRecordStatus(record);
		} catch (Exception ex) {
			logger.error("数据库更新record[" + record.getUuid() + "]发生异常!", ex);
		}
	}

	/**
	 * 更新record依赖
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void updateRecordRelations(KettleRecord record) throws KettleException {
		List<RowMetaAndData> tables = new ArrayList<RowMetaAndData>(record.getRelations().size());
		RowMetaAndData table;
		Date now = new Date();
		for (KettleRecordRelation relation : record.getRelations()) {
			table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_ID, ValueMetaInterface.TYPE_STRING),
					relation.getMetaid());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_META_TYPE, ValueMetaInterface.TYPE_STRING),
					relation.getType());
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_CREATETIME, ValueMetaInterface.TYPE_DATE), now);
			table.addValue(
					new ValueMeta(KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID, ValueMetaInterface.TYPE_STRING),
					relation.getMasterUUID());
			tables.add(table);

		}
		updateTableRows(KettleVariables.R_RECORD_DEPENDENT, tables, KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID);
	}

	/**
	 * 删除JOB
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public synchronized void deleteRecord(String uuid) throws KettleException {
		String sql0 = "DELETE FROM " + KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_JOB_RECORD_UUID
				+ " = ? ";
		String sql1 = "DELETE FROM " + KettleVariables.R_RECORD_DEPENDENT + " WHERE "
				+ KettleVariables.R_RECORD_DEPENDENT_MASTER_UUID_ID + " = ? ";
		deleteTableRow(sql0, ValueMetaInterface.TYPE_STRING, uuid);
		deleteTableRow(sql1, ValueMetaInterface.TYPE_STRING, uuid);
	}

	/**
	 * 删除JOB
	 * 
	 * @param uuid
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
	 * @return
	 * @throws KettleException
	 */
	public List<KettleRecord> allSchedulerRecord() throws KettleException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
				+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
				+ KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NOT NULL";
		List<Object[]> result = null;
		result = queryRows(sql);
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
	 * 获取所有需处理Job任务
	 * 
	 * @return
	 * @throws KettleException
	 */
	public List<KettleRecord> allHandleRecord() throws KettleException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
				+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
				+ KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NULL AND "
				+ KettleVariables.R_RECORD_STATUS + " in ('" + KettleVariables.RECORD_STATUS_RUNNING + "', '"
				+ KettleVariables.RECORD_STATUS_APPLY + "')";
		List<Object[]> result = null;
		result = queryRows(sql);
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
	public List<KettleRecord> allStopRecord() throws KettleException {
		String sql = "SELECT " + KettleVariables.R_JOB_RECORD_UUID + "," + KettleVariables.R_JOB_RECORD_ID_JOB + ","
				+ KettleVariables.R_JOB_RECORD_NAME_JOB + "," + KettleVariables.R_RECORD_ID_RUN + ","
				+ KettleVariables.R_RECORD_STATUS + "," + KettleVariables.R_RECORD_HOSTNAME + ","
				+ KettleVariables.R_RECORD_CREATETIME + "," + KettleVariables.R_RECORD_UPDATETIME + ","
				+ KettleVariables.R_RECORD_ERRORMSG + "," + KettleVariables.R_RECORD_CRON_EXPRESSION + " FROM "
				+ KettleVariables.R_JOB_RECORD + " WHERE " + KettleVariables.R_RECORD_CRON_EXPRESSION + " IS NULL AND "
				+ KettleVariables.R_RECORD_STATUS + " in ('" + KettleVariables.RECORD_STATUS_FINISHED + "', '"
				+ KettleVariables.RECORD_STATUS_ERROR + "');";
		List<Object[]> result = null;
		result = queryRows(sql);
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
