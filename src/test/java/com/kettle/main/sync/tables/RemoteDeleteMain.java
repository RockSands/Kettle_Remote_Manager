package com.kettle.main.sync.tables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;
import com.kettle.core.instance.metas.builder.SyncTablesDatasBuilder;

public class RemoteDeleteMain {
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("------------------------------");
		KettleMgrInstance.getInstance();
		List<String> flags = Arrays.asList("");
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
			if ("".equals(flag)) {
				source.setSql(
						"SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date "
								+ "FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no");
			} else {
				source.setSql(
						"SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date "
								+ "FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no AND first_name LIKE '"
								+ flag + "%'");
			}
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
			SyncTablesDatasBuilder builder = new SyncTablesDatasBuilder();
			System.out.println("-------------registe Delete----------------------");
			KettleResult resultTMP = KettleMgrInstance.getInstance()
					.registeJob(builder.source(source).target(target).createJob());
			Thread.sleep(10000);
			KettleMgrInstance.getInstance().deleteJob(resultTMP.getUuid());
			Thread.sleep(10000);
			System.out.println("-------------Running Delete----------------------");
			resultTMP = KettleMgrInstance.getInstance().registeJob(builder.source(source).target(target).createJob());
			KettleMgrInstance.getInstance().excuteJob(resultTMP.getUuid());
			while (true) {
				resultTMP = KettleMgrInstance.getInstance().queryJob(resultTMP.getUuid());
				if (KettleVariables.RECORD_STATUS_RUNNING.equals(resultTMP.getStatus())) {
					break;
				}
				if (KettleVariables.RECORD_STATUS_ERROR.equals(resultTMP.getStatus())
						|| KettleVariables.RECORD_STATUS_FINISHED.equals(resultTMP.getStatus())) {
					break;
				}
				Thread.sleep(1000);
			}
			if (KettleVariables.RECORD_STATUS_RUNNING.equals(resultTMP.getStatus())) {
				try {
					KettleMgrInstance.getInstance().deleteJob(resultTMP.getUuid());
				} catch (Exception ex) {
					System.out.println("==ErrMsg==>" + ex.getMessage());
				}
				Thread.sleep(5000);
				System.out.println("-----------deleteJobForce--------------");
				KettleMgrInstance.getInstance().deleteJobForce(resultTMP.getUuid());
			} else {
				KettleMgrInstance.getInstance().deleteJob(resultTMP.getUuid());
			}
		}
	}
}
