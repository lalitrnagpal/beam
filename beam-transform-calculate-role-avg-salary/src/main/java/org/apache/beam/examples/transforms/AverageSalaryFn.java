package org.apache.beam.examples.transforms;

import java.io.Serializable;

import org.apache.beam.examples.domain.Employee;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class AverageSalaryFn extends CombineFn<Employee, AverageSalaryFn.Accum, Double> implements Serializable {

	private static final long serialVersionUID = 1L;

	public static class Accum implements Serializable {
	    double sum = 0;
	    double count = 0;
	  }

	  @Override
	  public Accum createAccumulator() { return new Accum(); }

	  @Override
	  public Accum addInput(Accum accum, Employee employee) {
	      accum.sum += Double.parseDouble(employee.getSalary());
	      accum.count++;
	      return accum;
	  }

	  @Override
	  public Accum mergeAccumulators(Iterable<Accum> accums) {
	    Accum merged = createAccumulator();
	    for (Accum accum : accums) {
	      merged.sum += accum.sum;
	      merged.count += accum.count;
	    }
	    return merged;
	  }

	  @Override
	  public Double extractOutput(Accum accum) {
	    return ((double) accum.sum) / accum.count;
	  }
	  

}
