package org.apache.beam.examples.domain;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Employee implements Serializable {

	private String empid;
	private String prefix;
	private String first;
	private String middle;
	private String last;
	private String gender;
	private String mail;
	private String father;
	private String mother;
	private String maiden;
	private String dob;
	private String tob;
	private String age;
	private String doj;
	private String qoj;
	private String hoj;
	private String yoj;
	private String moj;
	private String moj_name;
	private String moj_name_short;
	private String doj_day;
	private String doj_week;
	private String doj_week_short;
	private String age_in_company;
	private String salary;
	
	private String fullname;
	
	@Override
	public boolean equals(Object obj) {
		Employee e1 = (Employee)obj;
		return this.getEmpid().equalsIgnoreCase(e1.getEmpid());
	}
	
	public String toString() {
		return empid + ", " + prefix + ", " + first + ", " + middle + ", " + last + ", " + gender + ", " + mail + ", " + father + ", " + ", " 
							+ mother + ", " + maiden + ", " + dob +", "+ tob + ", " + age + ", " + doj + ", " + qoj + ", " 
							+ hoj + ", " + yoj + ", " + moj + ", " + moj_name + ", " + moj_name_short + ", " + doj_day + ", "+ doj_week + ", " 
							+ ", "+ doj_week_short + ", " + age_in_company + ", " + salary;  
	}

	public Employee create(String line) {
		  try {
			  int i = 0;
			  String[] values = line.split(",");
			  this.setEmpid( values[0] );
			  this.setPrefix( values[1] );
			  this.setFirst( values[2] );
			  this.setMiddle( values[3] );
			  this.setLast( values[4] );
			  this.setGender( values[5] );
			  this.setMail( values[6] );
			  this.setFather( values[7] );
			  this.setMother( values[8] );
			  this.setMaiden( values[9] );
			  this.setDob( values[10] );
			  this.setTob( values[11] );
			  this.setAge( values[12] );
			  this.setDoj( values[13] );
			  this.setQoj( values[14] );
			  this.setHoj( values[15] );
			  this.setYoj( values[16] );
			  this.setMoj( values[17] );
			  this.setMoj_name( values[18] );
			  this.setMoj_name_short( values[19] );
			  this.setDoj_day( values[20] );
			  this.setDoj_week( values[21] );
			  this.setDoj_week_short( values[22] );
			  this.setAge_in_company( values[23] );
			  this.setSalary( values[24] );
		  
			  this.setFullname( this.getPrefix() + " " + this.getLast().toUpperCase() + " " + this.getMiddle() + " " + this.getFirst());
		  } catch (Exception e) {
			  System.out.println("Error while reading : "+e.getMessage());
			  e.printStackTrace();
		  }
		  return this;
	}
	
}
