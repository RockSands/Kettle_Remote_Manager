package com.kettle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExcuteMain {
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

		List<String> flags = Arrays.asList("", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N",
				"O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
		/*
		 * Source的TableName无效
		 */
		List<KettleDBTranDescribe> sources = new ArrayList<KettleDBTranDescribe>(flags.size());
		List<KettleDBTranDescribe> targets = new ArrayList<KettleDBTranDescribe>(flags.size());
		KettleDBTranDescribe source = null;
		KettleDBTranDescribe target = null;
		for (String flag : flags) {
			// 源配置
			source = new KettleDBTranDescribe();
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
			target = new KettleDBTranDescribe();
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
		List<KettleTransResult> results = new ArrayList<KettleTransResult>(flags.size());
		for (int i = 0; i < flags.size(); i++) {
			results.add(KettleMgrInstance.getInstance().createDataTransfer(sources.get(i), targets.get(i)));
		}
		/*
		 * Kettle的repository是实时的验证
		 */
		// for(int i=0;i<10 ; i++){
		// KettleMgrInstance.getInstance().connect();
		// System.out.println(KettleMgrInstance.getInstance().isConnected());
		// KettleMgrInstance.getInstance().disconnect();
		// }
		/*
		 * 以下为定时轮询,未完成,提供的查询接口是remoteTransStatus
		 */
		do {
			Thread.sleep(2000);
			System.out.println("------------------------------");
			for (KettleTransResult result : results) {
				result = KettleMgrInstance.getInstance().queryDataTransfer(result.getTransID());
				System.out.println("=DataTransfer[" + result.getTransID() + "]=>\n" + result.getStatus());
			}
			System.out.println("------------------------------");
		} while (true);
	}

}
