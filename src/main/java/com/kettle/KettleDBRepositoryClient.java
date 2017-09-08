package com.kettle;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;

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

	public KettleDBRepositoryClient(KettleDatabaseRepository repository) {
		this.repository = repository;
	}

	/**
	 * 持久化操作:查询
	 * 
	 * @throws KettleException
	 */
	public KettleTransResult queryTransRecord(String runID) {
		try {
			RowMetaAndData table = repository.connectionDelegate.getOneRow(KettleVariables.R_TRANS_RECORD,
					KettleVariables.R_RECORD_ID_RUN, new StringObjectId(runID));
			if (table == null || table.size() < 1) {
				return null;
			} else {
				KettleTransResult kettleTransResult = new KettleTransResult();
				kettleTransResult.setRunID(runID);
				kettleTransResult.setStatus(table.getString(KettleVariables.R_RECORD_STATUS, null));
				return kettleTransResult;
			}
		} catch (KettleException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 持久化操作:Insert
	 * 
	 * 
	 * @throws KettleException
	 */
	public void insertTransRecord(KettleTransBean kettleTransBean) throws KettleException {
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_NAME_TRANS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getTransName());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_ID_RUN, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getRunID());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getStatus());
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_HOSTNAME, ValueMetaInterface.TYPE_STRING),
				kettleTransBean.getHostname());
		repository.connectionDelegate.insertTableRow(KettleVariables.R_TRANS_RECORD, table);
	}

	/**
	 * 持久化操作:更新
	 * 
	 * @throws KettleException
	 */
	private void updateTransRecord(KettleTransBean KettleTransBean) throws KettleException {
		RowMetaAndData table = new RowMetaAndData();
		table.addValue(new ValueMeta(KettleVariables.R_RECORD_STATUS, ValueMetaInterface.TYPE_INTEGER),
				KettleTransBean.getStatus());
		repository.connectionDelegate.updateTableRow(KettleVariables.R_TRANS_RECORD, KettleVariables.R_RECORD_ID_RUN,
				table, new StringObjectId(KettleTransBean.getRunID()));
	}

	/**
	 * 持久化操作:删除
	 * 
	 * @throws KettleException
	 */
	private void deleteTransRecord(String runID) throws KettleException {
		repository.connectionDelegate.performDelete(
				"DELETE FROM " + KettleVariables.R_TRANS_RECORD + " WHERE " + KettleVariables.R_RECORD_ID_RUN + " = ?",
				new StringObjectId(runID));
	}
}
