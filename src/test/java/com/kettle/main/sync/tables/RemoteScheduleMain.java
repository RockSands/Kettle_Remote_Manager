package com.kettle.main.sync.tables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.metas.KettleTableMeta;
import com.kettle.core.metas.builder.SyncTablesDatasBuilder;

public class RemoteScheduleMain {

	public static void main(String[] args) throws KettleException, Exception {

		System.out.println("------------------------------");
		KettleMgrInstance.getInstance();
		// List<String> flags = Arrays.asList("A", "B", "C", "D", "E", "F", "G",
		// "H", "I", "J", "K", "L", "M", "N", "O",
		// "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
		List<String> flags = Arrays.asList("A", "B", "C", "D");
		/*
		 * Source的TableName无效
		 */
		List<KettleTableMeta> sources = new ArrayList<KettleTableMeta>(flags.size());
		List<KettleTableMeta> targets = new ArrayList<KettleTableMeta>(flags.size());
		KettleTableMeta source = null;
		KettleTableMeta target = null;
		for (String flag : flags) {
			// 源配置
			source = new KettleTableMeta();
			source.setType("MySQL");
			source.setHost("192.168.80.138");
			source.setPort("3306");
			source.setDatabase("employees");
			source.setUser("root");
			source.setPasswd("123456");
			source.setSql(
					"SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date "
							+ "FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no AND first_name LIKE '"
							+ flag + "%'");
			source.setColumns(Arrays.asList("emp_no", "dept_no", "first_name", "last_name", "birth_date"));
			source.setPkcolumns(Arrays.asList("emp_no", "dept_no"));
			sources.add(source);
			// 目标配置
			target = new KettleTableMeta();
			target.setType("MySQL");
			target.setHost("192.168.80.138");
			target.setPort("3306");
			target.setDatabase("person");
			target.setUser("root");
			target.setPasswd("123456");
			target.setColumns(Arrays.asList("empID", "deptID", "firstName", "lastName", "born"));
			target.setPkcolumns(Arrays.asList("empID", "deptID"));
			if ("".equals(flag)) {
				target.setSql("SELECT empID, deptID, firstName, lastName, born FROM target_employees");
				target.setTableName("target_employees");
			} else {
				target.setSql("SELECT empID, deptID, firstName, lastName, born FROM target_employees_" + flag);
				target.setTableName("target_employees_" + flag);
			}
			targets.add(target);
		}
		List<KettleResult> results = new ArrayList<KettleResult>(flags.size());
		KettleResult resultTMP;
		for (int i = 0; i < flags.size(); i++) {
			resultTMP = KettleMgrInstance.getInstance()
					.registeJob(SyncTablesDatasBuilder.newBuilder().source(source).target(target).createJob());
			KettleMgrInstance.getInstance().modifySchedule(resultTMP.getUuid(), "*/5 * * * * ?");
			results.add(resultTMP);
		}
		long now = System.currentTimeMillis();
		while ((System.currentTimeMillis() - now) / 1000 < 60) {
			for (KettleResult result : results) {
				result = KettleMgrInstance.getInstance().queryJob(result.getUuid());
				System.out.println("=1=status=>" + result.getStatus());
			}
			Thread.sleep(5000);
		}
		System.out.println("---------------10------------------");
		for (KettleResult result : results) {
			KettleMgrInstance.getInstance().modifySchedule(result.getUuid(), "*/10 * * * * ?");
		}
		now = System.currentTimeMillis();
		while ((System.currentTimeMillis() - now) / 1000 < 60) {
			for (KettleResult result : results) {
				result = KettleMgrInstance.getInstance().queryJob(result.getUuid());
				System.out.println("=2=status=>" + result.getStatus());
			}
			Thread.sleep(5000);
		}
		System.out.println("---------------DELETE------------------");
		for (KettleResult result : results) {
			KettleMgrInstance.getInstance().deleteJobForce(result.getUuid());
			Thread.sleep(60000);
		}
		System.out.println("---------------END------------------");
	}
}
