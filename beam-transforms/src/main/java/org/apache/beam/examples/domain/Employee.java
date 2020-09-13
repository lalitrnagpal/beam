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
@ToString
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
	private String weight;
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
	
	public String toString() {
		return empid + ", " + fullname + ", " + prefix + ", " + first + ", " + middle + ", " + last + ", " + gender + ", " + mail + ", " + father + ", " + ", " 
							+ mother + ", " + maiden + ", " + dob +", "+ tob + ", " + age + ", " + weight + ", " + doj + ", " + qoj + ", " 
							+ hoj + ", " + yoj + ", " + moj + ", " + moj_name + ", " + moj_name_short + ", " + doj_day + ", "+ doj_week + ", " 
							+ ", "+ doj_week_short + ", " + age_in_company + ", " + salary;  
	}
	
	
}
