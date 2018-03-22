package com.kettle.main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.metas.KettleTableMeta;
import com.kettle.core.metas.builder.SyncTablesDatasBuilder;

public class KettleRemoteQuery {
    public static void main(String[] args) throws Exception {
	KettleMgrInstance.getInstance();
	System.out.println("------------------------------");
	// List<String> flags = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H",
	// "I", "J", "K", "L", "M", "N", "O",
	// "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");
	List<String> flags = Arrays.asList("A");
	KettleTableMeta source = null;
	KettleTableMeta target = null;
	List<KettleJobEntireDefine> kettleJobEntireDefines = new ArrayList<KettleJobEntireDefine>();
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
	    kettleJobEntireDefines.add(SyncTablesDatasBuilder.newBuilder().source(source).target(target).createJob());
	}
	// 注册
	List<KettleResult> kettleResults = new ArrayList<KettleResult>();
	for (KettleJobEntireDefine kettleJobEntireDefine : kettleJobEntireDefines) {
	    kettleResults.add(KettleMgrInstance.getInstance().registeJob(kettleJobEntireDefine));
	}
	for (KettleResult result : kettleResults) {
	    KettleMgrInstance.getInstance().modifySchedule(result.getUuid(), "0 0 1 * * ?");
	}
	for (KettleResult result : kettleResults) {
	    KettleMgrInstance.getInstance().deleteJob(result.getUuid());
	}
	kettleResults.clear();
	for (KettleJobEntireDefine kettleJobEntireDefine : kettleJobEntireDefines) {
	    kettleResults.add(KettleMgrInstance.getInstance().applyScheduleJob(kettleJobEntireDefine, "0 0 1 * * ?"));
	}
	for (KettleResult result : kettleResults) {
	    KettleMgrInstance.getInstance().deleteJob(result.getUuid());
	}
    }
}
