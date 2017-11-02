package com.kettle.main.compare.tables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;

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
		List<String> flags = Arrays.asList("A", "B", "C");
		/*
		 * Source的TableName无效
		 */
		List<KettleTableMeta> sources = new ArrayList<KettleTableMeta>(flags.size());
		List<KettleTableMeta> targets = new ArrayList<KettleTableMeta>(flags.size());
		List<KettleTableMeta> newOptions = new ArrayList<KettleTableMeta>(flags.size());
		KettleTableMeta source = null;
		KettleTableMeta target = null;
		KettleTableMeta newOption = null;
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
			source.setPkcolumns(Arrays.asList("emp_no", "dept_no", "first_name"));
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
			target.setPkcolumns(Arrays.asList("empID", "deptID", "firstName"));
			target.setSql("SELECT empID, deptID, firstName, lastName, born FROM target_employees_" + flag);
			target.setTableName("target_employees_" + flag);
			targets.add(target);
			// new
			newOption = new KettleTableMeta();
			newOption.setType("MySQL");
			newOption.setHost("192.168.80.138");
			newOption.setPort("3306");
			newOption.setDatabase("person");
			newOption.setUser("root");
			newOption.setPasswd("123456");
			newOption.setColumns(Arrays.asList("firstName", "lastName", "empID", "deptID"));
			newOption.setSql("UPDATE target_employees_" + flag
					+ " SET firstName=?,lastName=? WHERE empID = ? AND deptID = ?");
			newOptions.add(newOption);
		}
		List<CreateCTDThread> createDataTransfers = new ArrayList<CreateCTDThread>(flags.size());
		ExecutorService threadPool = Executors.newFixedThreadPool(flags.size());
		for (int i = 0; i < flags.size(); i++) {
			CreateCTDThread cdt = new CreateCTDThread(sources.get(i), targets.get(i), newOptions.get(i));
			threadPool.execute(cdt);
			createDataTransfers.add(cdt);
		}
		threadPool.shutdown();
		do {
			Thread.sleep(10000);
			System.out.println("------------------------------");
			for (CreateCTDThread roll : createDataTransfers) {
				if (roll.getResult() != null) {
					KettleResult result = KettleMgrInstance.getInstance().queryResult(roll.getResult().getId());
					System.out.println("=DataTransfer[" + result.getId() + "]=>" + result.getStatus());
				}
			}
			System.out.println("------------------------------");
		} while (true);
	}
}
