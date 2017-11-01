package com.kettle.main.migrant.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleSQLSMeta;
import com.kettle.core.instance.metas.KettleTableMeta;

public class CreateTDMThread implements Runnable {
	KettleTableMeta source = null;

	KettleTableMeta target = null;

	KettleSQLSMeta success = null;

	KettleSQLSMeta error = null;

	KettleResult result = null;

	public CreateTDMThread(KettleTableMeta source, KettleTableMeta target, KettleSQLSMeta success,
			KettleSQLSMeta error) {
		super();
		this.source = source;
		this.target = target;
		this.success = success;
		this.error = error;
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			result = KettleMgrInstance.getInstance().tableDataMigration(source, target, success, error);
			System.out.println("==>registe used: " + (System.currentTimeMillis() - now));
			KettleMgrInstance.getInstance().excuteJob(result.getId());
			System.out.println("==>apply used: " + (System.currentTimeMillis() - now));
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}

	public void modifyCron(String newCron) throws KettleException {
		KettleMgrInstance.getInstance().modifySchedule(result.getId(), newCron);
	}

	public KettleResult getResult() {
		return result;
	}

}
