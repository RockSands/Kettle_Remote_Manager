package com.kettle.main.sync.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;

public class CreateSTDThread implements Runnable {
	KettleTableMeta source = null;

	KettleTableMeta target = null;

	String cron = null;

	KettleResult result = null;

	CreateSTDThread(KettleTableMeta source, KettleTableMeta target, String cron) {
		this.source = source;
		this.target = target;
		this.cron = cron;
	}

	@Override
	public void run() {
//		try {
//			if (result != null) {
//				result = KettleMgrInstance.getInstance().queryResult(result.getUuid());
//				// System.out.println("==>[" + result.getId() + "]状态: " +
//				// result.getStatus());
//			}
//			if (result == null) {
//				// long now = System.currentTimeMillis();
//				result = KettleMgrInstance.getInstance().registeSyncTablesDatas(source, target);
//				// System.out.println("==>registe used: " +
//				// (System.currentTimeMillis() - now));
//				KettleMgrInstance.getInstance().excuteJob(result.getUuid());
//				// System.out.println("==>apply used: " +
//				// (System.currentTimeMillis() - now));
//			}
//			if (KettleVariables.RECORD_STATUS_ERROR.equals(result.getStatus())
//					|| KettleVariables.RECORD_STATUS_FINISHED.equals(result.getStatus())) {
//				// KettleMgrInstance.getInstance().deleteJob(result.getUuid());
//				result = null;
//			}
//		} catch (KettleException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

	public void modifyCron(String newCron) throws KettleException {
		KettleMgrInstance.getInstance().modifySchedule(result.getUuid(), newCron);
	}

	public KettleResult getResult() {
		return result;
	}

}
