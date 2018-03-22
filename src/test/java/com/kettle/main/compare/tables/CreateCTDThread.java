package com.kettle.main.compare.tables;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.metas.KettleTableMeta;
import com.kettle.core.metas.builder.TablesCompareDatasBuilder;

public class CreateCTDThread implements Runnable {
    KettleTableMeta base = null;

    KettleTableMeta compare = null;

    KettleTableMeta newOption = null;

    String cron = null;

    KettleResult result = null;

    CreateCTDThread(KettleTableMeta base, KettleTableMeta compare, KettleTableMeta newOption) {
	this.base = base;
	this.compare = compare;
	this.newOption = newOption;
    }

    @Override
    public void run() {
	try {
	    long now = System.currentTimeMillis();
	    result = KettleMgrInstance.getInstance().registeJob(TablesCompareDatasBuilder.newBuilder().base(base)
		    .compare(compare).newOption(newOption).createJob());
	    System.out.println("==>registe used: " + (System.currentTimeMillis() - now));
	    KettleMgrInstance.getInstance().excuteJob(result.getUuid());
	    System.out.println("==>apply used: " + (System.currentTimeMillis() - now));
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
