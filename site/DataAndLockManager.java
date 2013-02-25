package site;
import java.util.*;
import java.io.*;
import transaction.Transaction;
import transaction.TransactionConstants;
/**
 * Keeps track of data items at a site and handles read, commit, abort and lock acquisition requests for variables available at the site 
 * serviced by the data and lock manager.
 * @author Ashish Walia
 *
 */
public class DataAndLockManager {
	private int dataAndLockManagerId =0;
	//Site serviced by this data and lock manager
	private Site site =null;
	
	/**
	 * Constructor for DataAndLockManager class.
	 * @param dataAndLockManagerId - An integer value that represents data and lock manager id.
	 * @param site - A site object which holds all the information of a given site.
	 */
	public DataAndLockManager(int dataAndLockManagerId,Site site)
	{
		this.dataAndLockManagerId=dataAndLockManagerId;
		this.site=site;
	}
	
	
	/**
	 * Method to get Data and Lock Manager Id.
	 * @author Ashish Walia
	 * @return int - An integer value representing Data and Lock Manager Id.
	 */
	public int getDataAndLockManagerId()
	{
		return dataAndLockManagerId;
	}
	
	/**
	 * Method which returns the object of Site serviced by the data and lock manager.
	 * @author Ashish Walia
	 * @return Site - An object of Site serviced by the data and lock manager
	 */
	public Site getSiteServicedByDataAndLockMgr()
	{
		return site;
	}
	
	/**
	 * Method to associate site with the data and lock manager serving that site.
	 * @author Ashish Walia
	 */
	public void addSiteServicedByDataAndLockMgr(Site site)
	{
		this.site=site;
	}
	
	/**
	 * Method which locks the variable in R mode
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read.
	 */
	public void acquireReadLock(Transaction trx, String variable)
	{
		if(trx!=null && getVariableVOFromMapOfVariablesVO(variable)!=null)
		{
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			if(variableVO!=null)
			{
				//Data and lock manager will handle lock information of variables only.
				variableVO.addReadLockOnVariable(trx);
			}
		}
	}
	/**
	 * Method which displays the reason why a transaction needs to be aborted.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 * @param out - A BufferedWriter object which write contents to the output file.
	 * @throws IOException
	 */
	public void displayReasonWhyTrxNeedsToBeAborted(Transaction trx,String variable,BufferedWriter out)throws IOException
	{
		if(site!=null && site.getSiteStatus() && trx!=null && variable!=null && !"".equals(variable) && out!=null)
		{
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			Set olderTrxs = new HashSet();
			if(variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty())
			{				
				Iterator itr = (Iterator)variableVO.getTrxsReadLockingVariable().iterator();				
				while(itr.hasNext())
				{
					Transaction readLockingTrx = (Transaction)itr.next();
					if(readLockingTrx!=null && (trx.getTrxTimeStamp()> readLockingTrx.getTrxTimeStamp()))
					{
						olderTrxs.add(readLockingTrx.getTrxId());						
					}
				}
				
			}
			if(variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && !variableVO.getTrxWriteLockingVariable().isEmpty())
			{				
				Iterator itr = (Iterator)variableVO.getTrxWriteLockingVariable().iterator();				
				while(itr.hasNext())
				{
					Transaction writeLockingTrx = (Transaction)itr.next();
					if(writeLockingTrx!=null && (trx.getTrxTimeStamp()> writeLockingTrx.getTrxTimeStamp()))
					{
						olderTrxs.add(writeLockingTrx.getTrxId());					
					}
				}
			}
			if(olderTrxs!=null && !olderTrxs.isEmpty())
			{

				System.out.print("Transaction "+trx.getTrxId()+" is younger than ");
				out.write("Transaction "+trx.getTrxId()+" is younger than ");
				Iterator itr =(Iterator)olderTrxs.iterator();
				while(itr.hasNext())
				{
					String trxName=(String)itr.next();
					System.out.print(trxName+"  ");
					out.write(trxName+"  ");
				}
				
				System.out.println("transaction(s) locking variable "+variable);
				out.write("transaction(s) locking variable "+variable);
				out.newLine();
			}
		}
	}

	/**
	 * Method which displays the reason why a transaction needs to be blocked.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 * @param out - A BufferedWriter object which write contents to the output file.
	 * @throws IOException
	 */
	public void displayReasonWhyTrxNeedsToBeBlocked(Transaction trx,String variable,BufferedWriter out)throws IOException
	{
		if(site!=null && site.getSiteStatus() && trx!=null && variable!=null && !"".equals(variable) && out!=null)
		{
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			Set youngerTrxs = new HashSet();
			if(variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty())
			{				
				Iterator itr = (Iterator)variableVO.getTrxsReadLockingVariable().iterator();				
				while(itr.hasNext())
				{
					Transaction readLockingTrx = (Transaction)itr.next();
					if(readLockingTrx!=null && (trx.getTrxTimeStamp()< readLockingTrx.getTrxTimeStamp()))
					{
						youngerTrxs.add(readLockingTrx.getTrxId());						
					}
				}				
			}
			if(variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && !variableVO.getTrxWriteLockingVariable().isEmpty())
			{				
				Iterator itr = (Iterator)variableVO.getTrxWriteLockingVariable().iterator();				
				while(itr.hasNext())
				{
					Transaction writeLockingTrx = (Transaction)itr.next();
					if(writeLockingTrx!=null && (trx.getTrxTimeStamp()< writeLockingTrx.getTrxTimeStamp()))
					{
						youngerTrxs.add(writeLockingTrx.getTrxId());					
					}
				}
			}
			if(youngerTrxs!=null && !youngerTrxs.isEmpty())
			{

				System.out.print("Transaction "+trx.getTrxId()+" is older than ");
				out.write("Transaction "+trx.getTrxId()+" is older than ");
				Iterator itr =(Iterator)youngerTrxs.iterator();
				while(itr.hasNext())
				{
					String trxName=(String)itr.next();
					System.out.print(trxName+"  ");
					out.write(trxName+"  ");
				}
				
				System.out.println("transaction(s) locking variable "+variable);
				out.write("transaction(s) locking variable "+variable);
				out.newLine();
			}
			
		}
	}

	/**
	 * Method which checks if read-lock can be acquired by a transaction on variable.
	 * Wait-die algorithm has been implemented here to prevent deadlock situation.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 */
	public int canReadLockBeAcquiredOnVariable(Transaction trx, String variable)
	{
		//Check whether site is up or not
		if(site!=null && !site.getSiteStatus())
		{
			return TransactionConstants.SITE_IS_DOWN;
		}
		//Check if the variable to be read is available on this site or not
		else if(variable!=null &&!"".equals(variable) && site!=null && site.getSiteStatus()&& site.getMapOfVariablesAndValuesAtSite()!=null && !site.getMapOfVariablesAndValuesAtSite().containsKey(variable))
		{
			return TransactionConstants.VARIABLE_NOT_FOUND_AT_SITE;
		}	
		//Check if variable is available to be read or not
		else if(!isVariableAvailableToBeReadFromSite(variable))
		{
			return TransactionConstants.VARIABLE_NOT_AVAILABLE_FOR_READ_OPERATION;
		}
		
		//Check if the read lock request can be granted to transaction or not		
		else if(trx!=null && getVariableVOFromMapOfVariablesVO(variable)!=null)
		{
			//Retrieve variablesVo for the corresponding variable
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			//If Transaction T already has read lock on variable x, no need to waste CPU cycles trying to acquire read lock
			if(variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty() && isTrxIdSameInReadOrWriteLockingTrxsSet(variableVO.getTrxsReadLockingVariable(),trx))
			{
				return TransactionConstants.READ_LOCK_ALREADY_ACQUIRED;
			}
			//If we have reached at this step, it means that the Transaction T doesn't have read lock on the variable x
			//If there is no write lock on variable x and Transaction T doesn't have read lock on variable x, it's safe to assign read lock to transaction T
			else if(variableVO!=null && ((variableVO.getTrxWriteLockingVariable()!=null && variableVO.getTrxWriteLockingVariable().isEmpty())||(variableVO.getTrxWriteLockingVariable()==null)))
			{
				return TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED;
			}
			//If we have reached at this step, it means Transaction T doesn't have read lock on variable x and the variable x is write locked by some transaction.
			//If T is the transaction having write lock on the variable x, then it's safe for the transaction T to acquire read lock on the variable x.
			else if (variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && variableVO.getTrxWriteLockingVariable().size()==1 && isTrxIdSameInReadOrWriteLockingTrxsSet(variableVO.getTrxWriteLockingVariable(),trx))
			{				
								
				return TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED;
				
			}
			
			//If we have reached at this step, it means that the Transaction T doesn't have read and write locks on the variable x		
			/*
			 **************************************************************************************
			 * Apply WAIT-DIE ALGORITHM. Determine if Transaction T needs to be blocked or aborted. 
			 **************************************************************************************
			 */			 
			else
			{
				//If age of Transaction T is less than age of any one of the transactions read/write locking variable x, then the transaction T must be aborted else it should be blocked
				
				//Check age of Transaction T with the age of transaction which has write lock on variable x
				if (trx!=null && variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && isTrxYoungerThanTrxsWriteOrReadLockingVariable(variableVO.getTrxWriteLockingVariable(),trx))
				{
					return TransactionConstants.ABORT_TRANSACTION;				
					
				}
				//Check age of Transaction T with the age of transactions which have read lock on variable x
				else if(trx!=null && variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty() && isTrxYoungerThanTrxsWriteOrReadLockingVariable(variableVO.getTrxsReadLockingVariable(),trx))
				{					
							return TransactionConstants.ABORT_TRANSACTION;						
				}
				//If age of Transaction T is greater than all the transaction read/write locking variable x, then block the transaction T
				else
				{
					return TransactionConstants.BLOCK_TRANSACTION;
				}
				
		}
			
		}
		
			return TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED;
		
	}
	
	/**
	 * Method which checks if transaction exists in a set of transactions read or write locking variable.
	 * @author Ashish Walia
	 * @param lockingTrxs - A set of transactions locking variable
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 */
	public boolean isTrxIdSameInReadOrWriteLockingTrxsSet(Set lockingTrxsSet, Transaction trx)
	{
		if(lockingTrxsSet!=null && trx!=null)
		{
			Iterator itr = lockingTrxsSet.iterator();
			while(itr.hasNext())
			{
				Transaction lockingTrx = (Transaction)itr.next();
				if(lockingTrx!=null && trx.getTrxId()!=null && lockingTrx.getTrxId()!=null && trx.getTrxId().equals(lockingTrx.getTrxId()))
				{
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Method which checks if transaction trying to acquire read/write lock is younger than the transaction(s) read/write locking variable.
	 * @author Ashish Walia
	 * @param lockingTrxs - A set of transactions locking variable
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 */
	public boolean isTrxYoungerThanTrxsWriteOrReadLockingVariable(Set lockingTrxs, Transaction trx)
	{
		if(lockingTrxs!=null && trx!=null)
		{
			Iterator itr = lockingTrxs.iterator();
			while(itr.hasNext())
			{
				Transaction lockingTrx = (Transaction)itr.next();
				if(lockingTrx!=null)
				{
					if(trx.getTrxTimeStamp()>lockingTrx.getTrxTimeStamp())
					{
						return true;
					}					
				}
			}			
		}
		return false;
	}
	
	/**
	 * Method which checks if write-lock can be acquired by a transaction on variable.
	 * Wait-die algorithm has been implemented here to prevent deadlock situation.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 */
	public int canWriteLockBeAcquiredOnVariable(Transaction trx, String variable)
	{
		//Check whether site is up or not
		if(site!=null && site.getSiteStatus()==false)
		{
			//System.out.println("Site "+site.getSiteId()+" is down");
			//return 0;
			//returns 400
			return TransactionConstants.SITE_IS_DOWN;
		}
		//Check if the variable to be written is available on this site or not
		else if(variable!=null &&!"".equals(variable) && site!=null && site.getSiteStatus() && site.getMapOfVariablesAndValuesAtSite()!=null && !site.getMapOfVariablesAndValuesAtSite().containsKey(variable))
		{
			//System.out.println("Variable "+variable+ "not found at site "+site.getSiteId());
			//return 0;	
			//returns 404
			return TransactionConstants.VARIABLE_NOT_FOUND_AT_SITE;
		}
		
		//Check if the write lock request can be granted to transaction or not
		else if(trx!=null && getVariableVOFromMapOfVariablesVO(variable)!=null)
		{
			//Retrieve variablesVo for the corresponding variable
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			//If Transaction T already has write lock on variable x, no need to waste CPU cycles trying to acquire write lock
			if(variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && isTrxIdSameInReadOrWriteLockingTrxsSet(variableVO.getTrxWriteLockingVariable(),trx))
			{
				//return 1;
				return TransactionConstants.WRITE_LOCK_ALREADY_ACQUIRED;
			}
			//If there are no read and write locks on variable x,it's safe to grant write lock to Transaction T
			else if(variableVO!=null && ((variableVO.getTrxsReadLockingVariable()!=null && variableVO.getTrxsReadLockingVariable().isEmpty()) ||(variableVO.getTrxsReadLockingVariable()==null))&& (variableVO.getTrxWriteLockingVariable()==null || (variableVO.getTrxWriteLockingVariable()!=null && variableVO.getTrxWriteLockingVariable().isEmpty())))
			{
				//return 1;
				return TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED;
			}
			//If there are no write locks on variable x but only Transaction T has read lock on variable x, then grant write lock on variable x to Transaction T
			else if(variableVO!=null && ((variableVO.getTrxWriteLockingVariable()!=null && variableVO.getTrxWriteLockingVariable().isEmpty())||(variableVO.getTrxWriteLockingVariable()== null)) && variableVO.getTrxsReadLockingVariable()!=null && variableVO.getTrxsReadLockingVariable().size()==1 && isTrxIdSameInReadOrWriteLockingTrxsSet(variableVO.getTrxsReadLockingVariable(),trx))
			{				
						return TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED;
			}
			/*
			 ************************************************************************************
			 * Apply WAIT-DIE ALGORITHM. Determine if Transaction T needs to be blocked or aborted. 
			 ************************************************************************************
			 */			 
			else 
			{
				//If age of Transaction T is less than age of any one of the transactions read/write locking variable x, then the transaction T must be aborted else it should be blocked
				
				//Check age of Transaction T with the age of transaction which has write lock on variable x
				if (trx!=null && variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && isTrxYoungerThanTrxsWriteOrReadLockingVariable(variableVO.getTrxWriteLockingVariable(),trx))
				{
								return TransactionConstants.ABORT_TRANSACTION;
				}
				//Check age of Transaction T with the age of transactions which have read lock on variable x
				else if(trx!=null && variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty() && isTrxYoungerThanTrxsWriteOrReadLockingVariable(variableVO.getTrxsReadLockingVariable(),trx))
				{
								return TransactionConstants.ABORT_TRANSACTION;
				}
				//If age of Transaction T is greater than all the transaction read/write locking variable x, then block the transaction T
				else
				{
					return TransactionConstants.BLOCK_TRANSACTION;
				}				
			}	
		}		
			//return 0;
			return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;		
	}
	/**
	 * Method which facilitates transaction to acquire write-lock on variable at the site service by the data and lock manager
	 * Wait-die algorithm has been implemented here to prevent deadlock situation.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 */
	public void acquireWriteLock(Transaction trx, String variable)
	{
		if(trx!=null && getVariableVOFromMapOfVariablesVO(variable)!=null)
		{
			VariablesVO variableVO = (VariablesVO)getVariableVOFromMapOfVariablesVO(variable);
			if(variableVO!=null)
			{
				//Data and lock manager will handle lock information of variables only.
				variableVO.addWriteLockOnVariable(trx);				
			}
		}
	}
	/**
	 * Method which facilitates transaction to commit value of variable at the site service by the data and lock manager
	 * Wait-die algorithm has been implemented here to prevent deadlock situation.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be read or write locked.
	 */
	public int commitValuesForVariablesLockedByTransaction(Transaction trx)
	{
		
		if(site!=null && !site.getSiteStatus())
		{
			return 0;
		}
		else if(getMapOfVariablesVO()!=null && trx.getLockedVariablesAndValuesMap()!=null)
		{
			boolean commited = false;
			//Match all the variables write locked by Transaction trx with the map of VariablesVO and update VariablesVO at this site
			Iterator itr = (Iterator)trx.getLockedVariablesAndValuesMap().entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					String variable = (String)pairs.getKey();
					int value = ((Integer)pairs.getValue()).intValue();
					Iterator iterator = (Iterator)getMapOfVariablesVO().entrySet().iterator();
					while(iterator.hasNext())
					{
						Map.Entry variableVOPairs = (Map.Entry)iterator.next();
						if(variableVOPairs!=null)
						{
							VariablesVO variableVO = (VariablesVO)variableVOPairs.getValue();
							if(variableVO!=null && variableVO.getVariable()!=null && variable!=null && variable.equals(variableVO.getVariable()))
							{
								//Commit the value at site
								variableVO.setValue(value);
								//A private hack to handle the case of recovery, where read operation on replicated variables are not allowed until a committed write takes place								
								if(variableVO.isVariableReplicated())
								{
									variableVO.makeVariableAvailableForReadOperation();
								}
								commited=true;
							}
						}
					}
					
				}
			}	
			
			//Release all the data locks held by this transaction on this site.
			releaseTransactionFromReadAndWriteLocksSet(trx);
			if(commited)
			{
			return 1;
			}
			else
			{
				return 0;
			}
		}
		else
		{
			return 0;
		}
	}
	/**
	 * Method which facilitates data and lock manager to release transaction from read locking and write locking transaction set maintained for each variable, in case the transaction is aborting.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 */
	public int abortTransaction(Transaction trx)
	{
		releaseTransactionFromReadAndWriteLocksSet(trx);
		return 0;
	}
	
	/**
	 * Method which facilitates data and lock manager to release transaction from read locking and write locking transaction set maintained for each variable.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 */
	public void releaseTransactionFromReadAndWriteLocksSet(Transaction trx)
	{
	 //Data Lock Manager will remove the Transaction T from read lock and write lock sets of all the variables available at this site
		if(getMapOfVariablesVO()!=null && !getMapOfVariablesVO().isEmpty())
		{
			Iterator iterator = (Iterator)getMapOfVariablesVO().entrySet().iterator();
			while(iterator.hasNext())
			{
				Map.Entry pairs = (Map.Entry)iterator.next();
				if(pairs!=null)
				{
					VariablesVO variable = (VariablesVO)pairs.getValue();
					if(variable!=null && variable.getTrxsReadLockingVariable()!=null)
					{
						variable.getTrxsReadLockingVariable().remove(trx);						
					}
					if(variable!=null && variable.getTrxWriteLockingVariable()!=null)
					{
						variable.getTrxWriteLockingVariable().remove(trx);
					}
				}
			}
		}		
	}
	/**
	 * Method which facilitates data and lock manager  to remove variable from Set of read locked and write locked variable, maintained for each transaction. This method comes in handy when a site fails.
	 * @author Ashish Walia
	 * @param variable - A String variable which needs to be released from transaction's read and write locks.
	 * @param lockingSet - A set of transactions read/write locking variable.
	 * @param replicated - A boolean value to indicate whether the String variable is replicated or not.
	 */
	public void releaseVariableFromTransactionReadAndWriteLocksSet(Set lockingSet, String variable, boolean replicated)
	{
		if(lockingSet!=null && variable!=null)
		{
			Iterator itr = (Iterator)lockingSet.iterator();
			while(itr.hasNext())
			{
				Transaction trx = (Transaction)itr.next();
				if(trx!=null && trx.getReadLockedVariables()!=null)
				{
					trx.getReadLockedVariables().remove(variable);
				}
				if(trx!=null && trx.getWriteLockedVariables()!=null && trx.getLockedVariablesAndValuesMap()!=null && !replicated)
				{
					trx.getWriteLockedVariables().remove(variable);
					trx.getLockedVariablesAndValuesMap().remove(variable);
				}				
			}			
		}
		
	}
	/**
	 * Method which returns map containing variables and values available at the site serviced by data and lock manager
	 * @author Ashish Walia
	 * @return Map - A map containing variables and values available at the site serviced by data and lock manager
	 */
	public Map getMapOfVariablesVO()
	{
		if(site!=null && site.getSiteStatus())
		{
			return site.getMapOfVariablesAndValuesAtSite();
		}
		else
		{
			return null;
		}
	}
	/**
	 * Method which returns VariableVO object.
	 * @author Ashish Walia
	 * @param variable - A string variable whose corresponding VariableVO object needs to be extracted.
	 * @return VariableVO - A VariableVO object.
	 */
	public VariablesVO getVariableVOFromMapOfVariablesVO(String variable)
	{
		if(variable!=null && !"".equals(variable) && site!=null && site.getSiteStatus() && site.getMapOfVariablesAndValuesAtSite()!=null && site.getMapOfVariablesAndValuesAtSite().containsKey(variable))
		{
			return (VariablesVO)site.getMapOfVariablesAndValuesAtSite().get(variable);
		}
		else
		{
			return null;
		}
	}
	/**
	 * Method which checks if the variable is available to be read from the site serviced by this data and lock manager.
	 * @author Ashish Walia
	 * @param variable - A string variable which needs to be read from the site.
	 */
	public boolean isVariableAvailableToBeReadFromSite(String variable)
	{
		if(variable!=null &&!"".equals(variable) && site!=null && site.getSiteStatus()&& site.getMapOfVariablesAndValuesAtSite()!=null && site.getMapOfVariablesAndValuesAtSite().containsKey(variable))
		{
			VariablesVO variableVO = (VariablesVO)site.getMapOfVariablesAndValuesAtSite().get(variable);
			if(variableVO!=null && variableVO.isVariableAvailableForReadOperation())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}
	/**
	 * Method which commits value of the variable at the site serviced by this data and lock manager.
	 * @author Ashish Walia
	 * @param variable - A string variable whose value needs to be committed at the site serviced by this data and lock manager.
	 * @param value - An integer value which needs to be committed at the site serviced by this data and lock manager.
	 */
	public void setValueOfVariableAtCommitTime(String variable, int value)
	{
		if(variable!=null && !"".equals(variable) && site!=null && site.getSiteStatus()==true && site.getMapOfVariablesAndValuesAtSite()!=null && site.getMapOfVariablesAndValuesAtSite().containsKey(variable)==true)
		{
			VariablesVO variableVO = (VariablesVO)site.getMapOfVariablesAndValuesAtSite().get(variable);
			if(variableVO!=null)
			{
				variableVO.setValue(value);
			}
		}
	}
	/**
	 * Method which fails site and releases lock information from all the variables at the site serviced by data and lock manager
	 * @author Ashish Walia
	 * @param out - A BuffereWriter object to write contents in the output file.
	 * @param tick - An integer value which updates the time stamp at which site failed.
	 */
	public void failSite(int tick, BufferedWriter out)throws IOException
	{
		if(site!=null && site.getSiteStatus()==false)
		{
			out.write("Site "+site.getSiteId()+" is already down.");
			out.newLine();
			System.out.println("Site "+site.getSiteId()+" is already down.");
		}
		else if(site!=null && site.getSiteStatus())
		{			
			//If a site fails, data and lock manager should forget about lock information
			if(getMapOfVariablesVO()!=null)
			{
				Iterator iterator = (Iterator)getMapOfVariablesVO().entrySet().iterator();
				while(iterator.hasNext())
				{
					Map.Entry pairs = (Map.Entry)iterator.next();
					if(pairs!=null)
					{
						VariablesVO variable = (VariablesVO)pairs.getValue();
						if(variable!=null && variable.getTrxsReadLockingVariable()!=null && !variable.getTrxsReadLockingVariable().isEmpty())
						{
							releaseVariableFromTransactionReadAndWriteLocksSet(variable.getTrxsReadLockingVariable(),variable.getVariable(),variable.isVariableReplicated());
							variable.getTrxsReadLockingVariable().clear();
						}
						if(variable!=null && variable.getTrxWriteLockingVariable()!=null && !variable.getTrxWriteLockingVariable().isEmpty())
						{
							releaseVariableFromTransactionReadAndWriteLocksSet(variable.getTrxWriteLockingVariable(),variable.getVariable(),variable.isVariableReplicated());
							variable.getTrxWriteLockingVariable().clear();
						}
					}
				}
				site.failSite(tick);
				out.write("Site "+site.getSiteId()+" failed.");
				out.newLine();
				System.out.println("Site "+site.getSiteId()+" failed.");
			}
		}
	}
	/**
	 * Method which recovers site and make non-replicated variables available for read and write operations. Replicated variables are rendered unavailable for read operation.
	 * @author Ashish Walia
	 * @param out - A BuffereWriter object to write contents in the output file.
	 * @param tick - An integer value which updates the time stamp at which site failed.
	 */
	public void recoverSite(BufferedWriter out)throws IOException
	{
		if(site!=null && site.getSiteStatus())
		{
			out.write("Site "+site.getSiteId()+" is already up.");
			out.newLine();
			System.out.println("Site "+site.getSiteId()+" is already up.");
		}
		else if(site!=null && site.getSiteStatus()==false)
		{
			site.recoverSite();
			//If a site recovers, Data and Lock Manager should make replicated variables unavailable for read operations
			if(getMapOfVariablesVO()!=null)
			{
				Iterator iterator = (Iterator)getMapOfVariablesVO().entrySet().iterator();
				while(iterator.hasNext())
				{
					Map.Entry pairs = (Map.Entry)iterator.next();
					if(pairs!=null)
					{
						VariablesVO variable = (VariablesVO)pairs.getValue();
						if(variable!=null && variable.isVariableReplicated())
						{
							variable.makeVariableUnavailableForReadOperation();
						}						
					}
				}
			}
			out.write("Site "+site.getSiteId()+" recovered.");
			out.newLine();
			System.out.println("Site "+site.getSiteId()+" recovered.");
		}
	}
}
