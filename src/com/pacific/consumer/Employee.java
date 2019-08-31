package com.pacific.consumer;

public class Employee {

	private String firstName;
	private String lastName;
	private int empId;

	public Employee(String firstName, String lastName, int empId) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.empId = empId;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public int getEmpId() {
		return empId;
	}

}
