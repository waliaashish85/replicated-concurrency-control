package transaction;
import java.util.*;
import operation.OperationVO;

/**
 * A place holder representing transaction at any given moment.
 * @author Ashish Walia
 *
 */

public class Transaction {
	//A String variable which reflects the status of transaction i.e. BLOCKED or ACTIVE or ABORTED
	private String transactionStatus=null;
	//A unique transaction id for each transaction.
	private String transactionId=null;
	//A unique timestamp for each transaction.
	private long timeStamp=0;
	//A boolean variable whose value reflects if the transaction is read-only transaction or not
	private boolean isReadOnly = false;
	private Map variablesAndValuesMapForROTrx = null;
	// A Map<Key,value>, key is Site Id and value is tick at which the site was accessed.
	private Map sitesAccessedSoFar = null;
	//A Map of containing values of all the variables that have been write-locked //by transaction
	private Map lockedVariablesAndValuesMap=null;
	// A set maintaining variables read-locked by transaction.
	private Set readLockedVariables=null;
	// A set maintaining variables write-locked by transaction.
	private Set writeLockedVariables=null;
	//A queue maintaining blocked operations
	private Queue <OperationVO> blockedOperationsQueue=null;
	
	public Transaction(String transactionId, boolean isReadOnly, int timeStamp)
	{
		this.transactionId=transactionId;
		this.isReadOnly=isReadOnly;
		transactionStatus="ACTIVE";
		this.timeStamp= timeStamp;
		readLockedVariables=new HashSet();
		writeLockedVariables=new HashSet();
		lockedVariablesAndValuesMap=new HashMap();
		sitesAccessedSoFar=new HashMap();
		if(isReadOnly)
		{
			variablesAndValuesMapForROTrx=new HashMap();
		}
	}
	//A method to get Queue of blocked operations
	public Queue<OperationVO> getBlockedOperationsQueue()
	{
		return blockedOperationsQueue;
	}
	//A method to add OperationVO object to queue of blocked operations.
	public void addOperationsToBlockedOperationsQueue(OperationVO operationVO)
	{
		if(blockedOperationsQueue==null)
		{
			blockedOperationsQueue=new LinkedList<OperationVO>();
		}
		if(operationVO!=null && !blockedOperationsQueue.contains(operationVO))
		{
			blockedOperationsQueue.add(operationVO);
		}
	}
	public boolean isOperationInBlockedOperationsQueue(OperationVO operationVO)
	{
		if(blockedOperationsQueue!=null && !blockedOperationsQueue.isEmpty() && operationVO!=null )
		{
			Iterator iterator = (Iterator)blockedOperationsQueue.iterator();
			while(iterator.hasNext())
			{
				OperationVO operation = (OperationVO)iterator.next();
				if(operation!=null && operation.getOperation()!=null && operation.getOperation().equals(operationVO.getOperation()))
				{
					return true;
				}
			}
		}		
			return false;		
	}
	//A method to remove OperationVO object from queue of blocked operations.
	public boolean removeOperationFromBlockedOperationsQueue(OperationVO operationVO)
	{
		if(blockedOperationsQueue!=null && operationVO!=null)
		{
			blockedOperationsQueue.remove(operationVO);
			return true;
		}
		else
		{
			return false;
		}		
	}
	public Map getVariablesAndValuesMapForROTrx()
	{
		return variablesAndValuesMapForROTrx;
	}
	public void setVariablesAndValuesMapForROTrx(String variable, int value)
	{
		if(variablesAndValuesMapForROTrx==null)
		{
			variablesAndValuesMapForROTrx=new HashMap();
		}
		variablesAndValuesMapForROTrx.put(variable, value);
	}
	
	public String getTransactionStatus()
	{
		return transactionStatus;
	}
	public void setTransactionStatus(String trxStatus)
	{
		if(TransactionConstants.ACTIVE_TRANSACTION_STATUS.equals(trxStatus) || TransactionConstants.BLOCKED_TRANSACTION_STATUS.equals(trxStatus) || TransactionConstants.ABORTED_TRANSACTION_STATUS.equals(trxStatus) )
		{
			transactionStatus=trxStatus;
		}
	}
	public boolean isTrxReadOnly()
	{
		return isReadOnly;
	}
	public long getTrxTimeStamp()
	{
		return timeStamp;
	}
	public String getTrxId()
	{
		return transactionId;
	}
	public void setLockedVariablesAndValuesMap (String variable, int value)
	{
		if(lockedVariablesAndValuesMap!=null)
		{
			lockedVariablesAndValuesMap.put(variable, value);
		}
		else
		{
			lockedVariablesAndValuesMap = new HashMap();
			lockedVariablesAndValuesMap.put(variable, value);
		}
	}	
	
	public Map getLockedVariablesAndValuesMap()
	{
		return lockedVariablesAndValuesMap;
	}
	public void clearLockedVariablesAndValuesMap()
	{
		if(lockedVariablesAndValuesMap!=null)
		{
			lockedVariablesAndValuesMap.clear();
		}
	}
	public Set getReadLockedVariables()
	{
		return readLockedVariables;
	}
	
	public void addReadLockedVariables(String variable)
	{
		if(variable!=null && !"".equals(variable) && readLockedVariables==null)
		{
			readLockedVariables = new HashSet();
			readLockedVariables.add(variable);
		}
		else if (readLockedVariables!=null && variable!=null && !"".equals(variable))
		{
			readLockedVariables.add(variable);
		}
		
	}
	public void clearReadLockedVariablesSet()
	{
		if(readLockedVariables!=null)
		{
			readLockedVariables.clear();
		}
	}
	public void clearBlockedOperationsQueue()
	{
		if(blockedOperationsQueue!=null)
		{
			blockedOperationsQueue.clear();
		}
	}
	public Set getWriteLockedVariables()
	{
		return writeLockedVariables;
	}
	public void addWriteLockedVariables(String variable)
	{
		if(writeLockedVariables==null && variable!=null && !"".equals(variable))
		{
			writeLockedVariables=new HashSet();
			writeLockedVariables.add(variable);
		}
		else if(writeLockedVariables!=null && variable!=null && !"".equals(variable))
		{
			writeLockedVariables.add(variable);
		}
	}
	public void clearWriteLockedVariablesSet()
	{
		if(writeLockedVariables!=null)
		{
			writeLockedVariables.clear();
		}
	}
	
	public Map getSitesAccessedSoFar()
	{
		return sitesAccessedSoFar;
	}
	
	public void addSiteAccessedInfoToSitesAccessedSoFarMap(int siteId, int tick)
	{
		
		if(sitesAccessedSoFar!=null)
		{
			if(!sitesAccessedSoFar.containsKey(siteId))
			{
				sitesAccessedSoFar.put(siteId,tick);
			}
		}
		else
		{
			sitesAccessedSoFar = new HashMap();
			sitesAccessedSoFar.put(siteId,tick);
			
		}
	}
	public void clearSitesAccessedSoFar()
	{
		if(sitesAccessedSoFar!=null)
		{
			sitesAccessedSoFar.clear();
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((transactionId == null) ? 0 : transactionId.hashCode());
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
		if (!(obj instanceof Transaction))
			return false;
		Transaction other = (Transaction) obj;
		if (transactionId == null) {
			if (other.transactionId != null)
				return false;
		} else if (!transactionId.equals(other.transactionId))
			return false;
		return true;
	}

	//public void addWriteLockedVariables(String variable)
	//{
		//if(variable!=null && !"".equals(variable) && writeLockedVariables==null)
		//{
			//writeLockedVariables = new HashSet();
		//}
		//else if (writeLockedVariables!=null && variable!=null && !"".equals(variable))
		//{
			//writeLockedVariables.add(variable);
		//}
		
	//}

}
