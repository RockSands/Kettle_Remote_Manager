package com.kettle.main;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleTransResult;
import com.kettle.core.instance.KettleDBTranDescribe;
import com.kettle.core.instance.KettleMgrInstance;

public class CreateSTDThread implements Runnable {
	KettleDBTranDescribe source = null;

	KettleDBTranDescribe target = null;

	String cron = null;

	KettleTransResult result = null;

	CreateSTDThread(KettleDBTranDescribe source, KettleDBTranDescribe target, String cron) {
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

	public KettleTransResult getResult() {
		return result;
	}

}
