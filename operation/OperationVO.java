package operation;
/**
 * A value object of operations which are parsed from input file and stored as OperationVO objects.
 * @author Ashish Walia
 *
 */
public class OperationVO {
private String operation = null;
private int typeOfOperation=0;
private int tick=0;
private String variable=null;
private String trxName=null;
private int value=0;

public OperationVO(String operation, int typeOfOperation,int tick)
{
	this.typeOfOperation=typeOfOperation;
	this.operation=operation;
	this.tick=tick;
}
/**
 * @return the operation
 */
public String getOperation() {
	return operation;
}


/**
 * @param operation the operation to set
 */
public void setOperation(String operation) {
	this.operation = operation;
}

/**
 * @return the typeOfOperation
 */
public int getTypeOfOperation() {
	return typeOfOperation;
}

/**
 * @param typeOfOperation the typeOfOperation to set
 */
public void setTypeOfOperation(int typeOfOperation) {
	this.typeOfOperation = typeOfOperation;
}

/**
 * @return the tick
 */
public int getTick() {
	return tick;
}
/**
 * @param tick the tick to set
 */
public void setTick(int tick) {
	this.tick = tick;
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
 * @return the trxName
 */
public String getTrxName() {
	return trxName;
}
/**
 * @param trxName the trxName to set
 */
public void setTrxName(String trxName) {
	this.trxName = trxName;
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
/* (non-Javadoc)
 * @see java.lang.Object#hashCode()
 */
@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((operation == null) ? 0 : operation.hashCode());
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
	if (!(obj instanceof OperationVO))
		return false;
	OperationVO other = (OperationVO) obj;
	if (operation == null) {
		if (other.operation != null)
			return false;
	} else if (!operation.equals(other.operation))
		return false;
	return true;
}

}
