package com.kettle.main.migrant.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.metas.KettleSQLSMeta;
import com.kettle.core.metas.KettleTableMeta;
import com.kettle.core.metas.builder.TableDataMigrationBuilder;

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
			KettleJobEntireDefine jobEntire = TableDataMigrationBuilder.newBuilder().source(source).target(target).success(success).error(error).createJob();
			result = KettleMgrInstance.getInstance().registeJob(jobEntire);
			System.out.println("==>registe used: " + (System.currentTimeMillis() - now));
			KettleMgrInstance.getInstance().excuteJob(result.getUuid());
			System.out.println("==>apply used: " + (System.currentTimeMillis() - now));
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}

	public void modifyCron(String newCron) throws KettleException {
		KettleMgrInstance.getInstance().modifySchedule(result.getUuid(), newCron);
	}

	public KettleResult getResult() {
		return result;
	}

}
