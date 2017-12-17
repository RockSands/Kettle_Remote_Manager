package com.kettle.main.migrant.tables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.metas.KettleSQLSMeta;
import com.kettle.core.metas.KettleTableMeta;

public class RemoteMain {
	/**
	 * 需求: 将源数据库(jdbc:mysql://192.168.80.138:3306/employees 用户名:root 密码: 123456)
	 * 的SQL --- SELECT employees.emp_no, dept_emp.dept_no, employees.first_name,
	 * employees.last_name, employees.birth_date FROM employees, dept_emp WHERE
	 * employees.emp_no = dept_emp.emp_no AND first_name LIKE 'C%'
	 * 导入到目标数据库(jdbc:mysql://192.168.80.138:3306/person 用户名:root 密码:
	 * 123456)的表target_employees("empID", "deptID", "firstName", "lastName",
	 * "born" 主键为"empID"与"deptID")
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("------------------------------");
		KettleMgrInstance.getInstance();
		List<String> flags = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
				"P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
		/*
		 * Source的TableName无效
		 */
		List<KettleTableMeta> sources = new ArrayList<KettleTableMeta>(flags.size());
		List<KettleTableMeta> targets = new ArrayList<KettleTableMeta>(flags.size());
		List<KettleSQLSMeta> successes = new ArrayList<KettleSQLSMeta>(flags.size());
		List<KettleSQLSMeta> errors = new ArrayList<KettleSQLSMeta>(flags.size());
		KettleTableMeta source = null;
		KettleTableMeta target = null;
		KettleSQLSMeta success = null;
		KettleSQLSMeta error = null;
		for (String flag : flags) {
			// 源配置
			source = new KettleTableMeta();
			source.setType("MySQL");
			source.setHost("192.168.80.138");
			source.setPort("3306");
			source.setDatabase("employees");
			source.setUser("root");
			source.setPasswd("123456");
			source.setColumns(Arrays.asList("emp_no", "dept_no", "first_name", "last_name", "birth_date"));
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
			if ("".equals(flag)) {
				target.setTableName("target_employees");
			} else {
				target.setTableName("target_employees_" + flag);
			}
			targets.add(target);
			// 成功操作
			success = new KettleSQLSMeta();
			success.setType("MySQL");
			success.setHost("192.168.80.138");
			success.setPort("3306");
			success.setDatabase("person");
			success.setUser("root");
			success.setPasswd("123456");
			success.setSqls(Arrays.asList("UPDATE " + "target_employees_" + flag + " SET firstName = 'success'",
					"UPDATE " + "target_employees_" + flag + " SET lastName = 'success'"));
			successes.add(success);
			// 异常操作
			error = new KettleSQLSMeta();
			error.setType("MySQL");
			error.setHost("192.168.80.138");
			error.setPort("3306");
			error.setDatabase("person");
			error.setUser("root");
			error.setPasswd("123456");
			error.setSqls(Arrays.asList("UPDATE " + "target_employees_" + flag + " SET firstName = 'error'",
					"UPDATE " + "target_employees_" + flag + " SET lastName = 'error'"));
			errors.add(error);
		}
		List<CreateTDMThread> createDataTransfers = new ArrayList<CreateTDMThread>(flags.size());
		ExecutorService threadPool = Executors.newFixedThreadPool(flags.size());
		for (int i = 0; i < flags.size(); i++) {
			// CreateSTDThread cdt = new CreateSTDThread(sources.get(i),
			// targets.get(i), "0 */1 * * * ?");
			CreateTDMThread cdt = new CreateTDMThread(sources.get(i), targets.get(i), successes.get(i), errors.get(i));
			threadPool.execute(cdt);
			// if (i % 10 == 0) {
			// Thread.sleep(20000);
			// }
			createDataTransfers.add(cdt);
		}
		threadPool.shutdown();
		do {
			Thread.sleep(10000);
			System.out.println("------------------------------");
			for (CreateTDMThread createDataTransfer : createDataTransfers) {
				if (createDataTransfer.getResult() != null) {
					KettleResult result = KettleMgrInstance.getInstance()
							.queryJob(createDataTransfer.getResult().getUuid());
					System.out.println("=DataTransfer[" + result.getUuid() + "]=>" + result.getStatus());
				}
			}
			System.out.println("------------------------------");
		} while (true);
	}
}
