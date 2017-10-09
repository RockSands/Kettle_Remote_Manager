package com.kettle;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;

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
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public void saveTransMeta(TransMeta transMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		transMeta.setRepositoryDirectory(repositoryDirectory);
		repository.save(transMeta, "1", Calendar.getInstance(), null, true);
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
		TransMeta transMeta = this.getTransMeta(transID);
		if (transMeta != null) {
			repository.deleteTransformation(new LongObjectId(transID));
			for (String databaseName : transMeta.getDatabaseNames()) {
				repository.deleteDatabaseMeta(databaseName);
			}
		}
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
	public KettleTransBean queryTransRecord(long transID) {
		if (!repository.isConnected()) {
			connect();
		}
		try {
			RowMetaAndData table = repository.connectionDelegate.getOneRow(KettleVariables.R_TRANS_RECORD,
					KettleVariables.R_RECORD_ID_TRANS, new LongObjectId(transID));
			if (table == null || table.size() < 1) {
				return null;
			} else {
				KettleTransBean kettleTransBean = new KettleTransBean();
				kettleTransBean.setTransId(transID);
				kettleTransBean.setTransName(table.getString(KettleVariables.R_RECORD_NAME_TRANS, null));
				kettleTransBean.setRunID(table.getString(KettleVariables.R_RECORD_ID_RUN, null));
				kettleTransBean.setStatus(table.getString(KettleVariables.R_RECORD_STATUS, null));
				kettleTransBean.setHostname(table.getString(KettleVariables.R_RECORD_HOSTNAME, null));
				return kettleTransBean;
			}
		} catch (KettleException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 持久化操作:Insert转换记录
	 * 
	 * 
	 * @throws KettleException
	 */
	public void insertTransRecord(KettleTransBean kettleTransBean) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_TRANS, ValueMetaInterface.TYPE_INTEGER),
				kettleTransBean.getTransId());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_NAME_TRANS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getTransName());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getHostname());
		repository.connectionDelegate.insertTableRow(KettleVariables.R_TRANS_RECORD, table);
		repository.commit();
	}

	/**
	 * 持久化操作:更新转换记录
	 * 
	 * @throws KettleException
	 */
	public void updateTransRecord(KettleTransBean kettleTransBean) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getStatus());
		repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD, KettleVariables.R_RECORD_ID_TRANS,
				table, new LongObjectId(kettleTransBean.getTransId()));
		repository.commit();
	}

	/**
	 * 持久化操作:更新转换记录
	 * 
	 * @throws KettleException
	 */
	public void updateTransRecords(List<KettleTransBean> kettleTransBeans) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table;
		for (KettleTransBean kettleTransBean : kettleTransBeans) {
			table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					kettleTransBean.getStatus());
			repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD,
					KettleVariables.R_RECORD_ID_TRANS, table, new LongObjectId(kettleTransBean.getTransId()));
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
				+ KettleVariables.R_RECORD_ID_TRANS + " = ?", new LongObjectId(transID));
		repository.commit();
	}

	public List<KettleTransBean> allRunningRecord(String hostname) throws KettleDatabaseException {
		if (!repository.isConnected()) {
			connect();
		}
		String sql = "SELECT " + KettleVariables.R_RECORD_ID_TRANS + "," + KettleVariables.R_RECORD_NAME_TRANS + ","
				+ KettleVariables.R_RECORD_ID_RUN + "," + KettleVariables.R_RECORD_STATUS + ","
				+ KettleVariables.R_RECORD_HOSTNAME + " FROM " + KettleVariables.R_TRANS_RECORD + " WHERE "
				+ KettleVariables.R_RECORD_HOSTNAME + " = '" + hostname + "' AND " + KettleVariables.R_RECORD_STATUS
				+ " = '" + KettleVariables.RECORD_STATUS_RUNNING + "'";
		List<Object[]> result = repository.connectionDelegate.getRows(sql, -1);
		List<KettleTransBean> kettleTransBeans = new LinkedList<KettleTransBean>();
		if (result == null || result.isEmpty()) {
			return kettleTransBeans;
		}
		KettleTransBean bean = null;
		for (Object[] record : result) {
			bean = new KettleTransBean();
			bean.setTransId((Long) record[0]);
			bean.setTransName((String) record[1]);
			bean.setRunID((String) record[2]);
			bean.setStatus((String) record[3]);
			bean.setHostname((String) record[4]);
			kettleTransBeans.add(bean);
		}
		return kettleTransBeans;
	}

	/**
	 * 持久化操作:Insert转换记录
	 * 
	 * 
	 * @throws KettleException
	 */
	public void insertClusterRecord(KettleTransBean kettleTransBean) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_TRANS, ValueMetaInterface.TYPE_INTEGER),
				kettleTransBean.getTransId());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_NAME_TRANS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getTransName());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getHostname());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_CLUSTERNAME, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getClusterName());
		repository.connectionDelegate.insertTableRow(KettleVariables.R_TRANS_RECORD, table);
		/*
		 * 子集合
		 */
		RowMetaAndData splitTable = null;
		LongObjectId id = null;
		for (KettleTransSplitBean bean : kettleTransBean.getClusterSplits()) {
			splitTable = new RowMetaAndData();
			id = repository.connectionDelegate.getNextID(KettleVariables.R_TRANS_RECORD_SPLIT,
					KettleVariables.R_TRANS_RECORD_SPLIT_ID);
			bean.setSplitId(id.longValue());
			splitTable.addValue(new ValueMeta(KettleVariables.R_TRANS_RECORD_SPLIT_ID, ValueMetaInterface.TYPE_INTEGER),
					id.longValue());
			splitTable.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_TRANS, ValueMetaInterface.TYPE_INTEGER),
					kettleTransBean.getTransId());
			splitTable.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					kettleTransBean.getStatus());
			splitTable.addValue(new ValueMeta(KettleVariables.R_RECORD_CLUSTERNAME, ValueMetaInterface.TYPE_STRING),
					kettleTransBean.getClusterName());
			splitTable.addValue(new ValueMeta(KettleVariables.R_RECORD_NAME_TRANS, ValueMetaInterface.TYPE_STRING),
					bean.getTransName());
			splitTable.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
					bean.getHostname());
			repository.connectionDelegate.insertTableRow(KettleVariables.R_TRANS_RECORD_SPLIT, splitTable);
		}
		repository.commit();
	}

	/**
	 * 持久化操作:更新转换记录
	 * 
	 * @throws KettleException
	 */
	public void updateClusterTransSplit(KettleTransSplitBean kettlesplitBean) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				kettlesplitBean.getStatus());
		repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD_SPLIT,
				KettleVariables.R_TRANS_RECORD_SPLIT_ID, table, new LongObjectId(kettlesplitBean.getSplitId()));
		repository.commit();
	}

	/**
	 * 持久化操作:更新转换记录
	 * 
	 * @throws KettleException
	 */
	public void updateClusterTransSplits(List<KettleTransSplitBean> kettlesplitBeans) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
		RowMetaAndData table = new RowMetaAndData();
		for (KettleTransSplitBean kettlesplitBean : kettlesplitBeans) {
			table = new RowMetaAndData();
			table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
					kettlesplitBean.getStatus());
			repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD_SPLIT,
					KettleVariables.R_TRANS_RECORD_SPLIT_ID, table, new LongObjectId(kettlesplitBean.getSplitId()));
		}
		repository.commit();
	}
}
