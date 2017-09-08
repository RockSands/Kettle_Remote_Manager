package com.kettle;

import java.util.Arrays;

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
		/*
		 * Source的TableName无效
		 */
		KettleDBTranDescribe source = new KettleDBTranDescribe();
		source.setType("MySQL");
		source.setHost("192.168.80.138");
		source.setPort("3306");
		source.setDatabase("employees");
		source.setUser("root");
		source.setPasswd("123456");
		source.setSql(
				"SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no AND first_name LIKE 'Ch%'");
		source.setColumns(Arrays.asList("emp_no", "dept_no", "first_name", "last_name", "birth_date"));
		source.setPkcolumns(Arrays.asList("emp_no", "dept_no"));

		KettleDBTranDescribe target = new KettleDBTranDescribe();
		target.setType("MySQL");
		target.setHost("192.168.80.138");
		target.setPort("3306");
		target.setDatabase("person");
		target.setUser("root");
		target.setPasswd("123456");
		target.setColumns(Arrays.asList("empID", "deptID", "firstName", "lastName", "born"));
		target.setPkcolumns(Arrays.asList("empID", "deptID"));
		target.setSql("SELECT empID, deptID, firstName, lastName, born FROM target_employees");
		target.setTableName("target_employees");
		KettleTransResult result = KettleMgrInstance.getInstance().createDataTransfer(source, target);
		System.out.println("完成");
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
		// Thread.sleep(5000);
		// do {// 睡1分钟
		// Thread.sleep(5000);
		// result =
		// KettleMgrInstance.getInstance().queryTransStatus(result.getTransID());
		// System.out.println("------------------------------");
		// System.out.println("=status:Msg=>\n" + result.getStatus());
		// } while (true);
	}

}
