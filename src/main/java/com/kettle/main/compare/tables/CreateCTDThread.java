package com.kettle.main.compare.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;

public class CreateCTDThread implements Runnable {
	KettleTableMeta source = null;

	KettleTableMeta target = null;

	KettleTableMeta newOption = null;

	String cron = null;

	KettleResult result = null;

	CreateCTDThread(KettleTableMeta source, KettleTableMeta target, KettleTableMeta newOption) {
		this.source = source;
		this.target = target;
		this.newOption = newOption;
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			result = KettleMgrInstance.getInstance().registeCompareTablesDatas(source, target, newOption);
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
