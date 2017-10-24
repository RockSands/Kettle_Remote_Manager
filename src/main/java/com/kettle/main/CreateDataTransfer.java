package com.kettle.main;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleTransResult;
import com.kettle.core.instance.KettleDBTranDescribe;
import com.kettle.core.instance.KettleMgrInstance;

public class CreateDataTransfer implements Runnable {
	KettleDBTranDescribe source = null;

	KettleDBTranDescribe target = null;

	KettleTransResult result = null;

	CreateDataTransfer(KettleDBTranDescribe source, KettleDBTranDescribe target) {
		this.source = source;
		this.target = target;
	}

	@Override
	public void run() {
		try {
			long now = System.currentTimeMillis();
			result = KettleMgrInstance.getInstance().createDataTransfer(source, target);
			System.out.println("==>SendTransfer used: " + (System.currentTimeMillis() - now));
		} catch (KettleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
