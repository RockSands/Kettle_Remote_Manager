package com.kettle.main.sync.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;
import com.kettle.core.instance.metas.builder.SyncTablesDatasBuilder;

public class CreateSTDThread implements Runnable {
	KettleTableMeta source = null;

	KettleTableMeta target = null;

	String cron = null;

	KettleResult result = null;

	KettleJobEntireDefine kjed = null;

	CreateSTDThread(KettleTableMeta source, KettleTableMeta target, String cron) {
		this.source = source;
		this.target = target;
		this.cron = cron;
	}

	@Override
	public void run() {
		try {
			if (result != null) {
				// long now = System.currentTimeMillis();
				result = KettleMgrInstance.getInstance().queryJob(result.getUuid());
				// System.out.println("==>[" + result.getUuid() + "]状态: " +
				// result.getStatus());
				// System.out.println("==>query used: " +
				// (System.currentTimeMillis() - now));
			}
			if (result == null) {
				SyncTablesDatasBuilder builder = new SyncTablesDatasBuilder();
				builder.source(source);
				builder.target(target);
				kjed = builder.createJob();
				long now = System.currentTimeMillis();
				result = KettleMgrInstance.getInstance().registeJob(kjed);
				// System.out.println("==>registe used: " +
				// (System.currentTimeMillis() - now));
				now = System.currentTimeMillis();
				KettleMgrInstance.getInstance().excuteJob(result.getUuid());
				System.out.println("==>apply used: " + (System.currentTimeMillis() - now));
			}
			if (KettleVariables.RECORD_STATUS_ERROR.equals(result.getStatus())
					|| KettleVariables.RECORD_STATUS_FINISHED.equals(result.getStatus())) {
				//KettleMgrInstance.getInstance().deleteJob(result.getUuid());
				result = null;
			}
		} catch (KettleException e) {
			e.printStackTrace();
		} catch (Exception e) {
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
