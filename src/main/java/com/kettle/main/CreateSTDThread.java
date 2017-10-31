package com.kettle.main;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleSelectMeta;
import com.kettle.core.instance.KettleMgrInstance;

public class CreateSTDThread implements Runnable {
	KettleSelectMeta source = null;

	KettleSelectMeta target = null;

	String cron = null;

	KettleResult result = null;

	CreateSTDThread(KettleSelectMeta source, KettleSelectMeta target, String cron) {
		this.source = source;
		this.target = target;
		this.cron = cron;
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			if (cron != null) {
				result = KettleMgrInstance.getInstance().syncTablesDataSchedule(source, target, cron);
			} else {
				result = KettleMgrInstance.getInstance().syncTablesDatas(source, target);
			}
			System.out.println("==>SendTransfer used: " + (System.currentTimeMillis() - now));
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
