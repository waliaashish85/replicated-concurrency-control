package site;
import java.util.*;
import transaction.Transaction;
/**
 * A place holder for variables and and its value, maintained at each site
 * @author Ashish Walia
 *
 */
public class VariablesVO implements Comparable<VariablesVO>{
	private String variable = null;	
	private int initialValue=0;
	private boolean isVariableReplicated = false;
	private boolean isVariableAvailableForReadOperation = true;
	private int value =0;
	//private Transaction writeLockedByTrx = null;
	private Set readLockedByTrxs = null;
	//Although, there will be only one transaction having write locked variable, we need a data structure that
	//can add or remove Transaction object without nullifying the original Transaction object.
	private Set writeLockedByTrx = null;
	public VariablesVO(String var,int value)
	{
		if(var!=null)
		{
			variable=var;
			if("x2".equals(var)|| "x4".equals(var) || "x6".equals(var) || "x8".equals(var) || "x10".equals(var)|| "x12".equals(var) || "x14".equals(var)|| "x16".equals(var)|| "x18".equals(var)|| "x20".equals(var))
			{
				isVariableReplicated=true;
			}
			this.value = value;
			this.initialValue=value;
		}
	}
	public void addReadLockOnVariable(Transaction trx) {
		
		if(readLockedByTrxs==null)
		{
			readLockedByTrxs=new HashSet();
			
		}
		if(trx!=null)
		{
			readLockedByTrxs.add(trx);
		}
	}
	public void addWriteLockOnVariable(Transaction trx) {
		if(writeLockedByTrx==null)
		{
			writeLockedByTrx = new HashSet();
		}
		if(trx!=null)
		{
			writeLockedByTrx.add(trx);
		}
	}
	
	public Set getTrxsReadLockingVariable() {
		return readLockedByTrxs;
	}
	
	public Set getTrxWriteLockingVariable()
	{
		return writeLockedByTrx;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((variable == null) ? 0 : variable.hashCode());
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof VariablesVO))
			return false;
		VariablesVO other = (VariablesVO) obj;
		if (variable == null) {
			if (other.variable != null)
				return false;
		} else if (!variable.equals(other.variable))
			return false;
		return true;
	}
	/**
	 * @return the variable
	 */
	public String getVariable() {
		return variable;
	}
	/**
	 * @param variable the variable to set
	 */
	public void setVariable(String variable) {
		this.variable = variable;
	}
	/**
	 * @return the value
	 */
	public int getValue() {
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(int value) {
		this.value = value;
	}
	
	public boolean isVariableReplicated()	
	{
		return isVariableReplicated;
	}
	public boolean isVariableAvailableForReadOperation()
	{
		return isVariableAvailableForReadOperation;
	}
	public void makeVariableUnavailableForReadOperation()
	{
		isVariableAvailableForReadOperation=false;
	}
	public void makeVariableAvailableForReadOperation()
	{
		isVariableAvailableForReadOperation=true;
	}
	public int getInitialValue()
	{
		return initialValue;
	}
	public int compareTo(VariablesVO variableVO) {
		if(variableVO!=null)
		{
			return (this.initialValue - variableVO.getInitialValue());
		}
        
        return 0;
    }

}
