package transaction;
import java.util.*;
import operation.OperationConstants;
import operation.OperationVO;
import site.DataAndLockManager;
import site.VariablesVO;
import java.io.*;

/**
 * Manages active and blocked transactions and services read, write, commit, block and abort requests.
 * @author Ashish Walia 
 */
public class TransactionManager {
	//A map of all the data and lock managers available
	private Map dataLockManagersMap = null;	
	//A map of all the transactions
	private Map trxsMap = null;
	//Lis of blocked transactions
	private List blockedTrxsList=null;
	//Set of variables that have already been read by read-on transaction
	private Set variablesReadByReadOnlyTrx=null;
	//A hack to prevent sending lock acquisition requests to sites in case the lock needs to be acquired on non-replicated variables
	private String[] nonReplicatedVariables={"x1","x3","x5","x7","x9","x11","x13","x15","x17","x19"};
	private int[] siteIdForNonReplicatedVariables={2,4,6,8,10,2,4,6,8,10};
	/**
	 * A constructor for TransactionManager class
	 * @param dataLockManagersMap - A map containing all the data and lock managers' instances
	 * @author Ashish Walia
	 */
	public TransactionManager(Map dataLockManagersMap)
	{
		this.dataLockManagersMap=dataLockManagersMap;
		trxsMap=new HashMap();
	}
	/**
	 * Method to get a Set of variables read by read-only transaction.
	 * @return Set - A set of variables read by read-only transaction.
	 * @author Ashish Walia
	 */
	public Set getVariablesReadByReadOnlyTrx()
	{
		return variablesReadByReadOnlyTrx;
	}
	/**
	 * A method to add variables to the Set of variables read by read-only transaction.
	 * @param variable - A string variable which needs to be added to the Set of variables read by read-only transaction.
	 * @author Ashish Walia
	 */
	public void addVariableToVariablesReadByReadOnlyTrx(String variable)
	{
		if(variablesReadByReadOnlyTrx==null)
		{
			variablesReadByReadOnlyTrx=new HashSet();
		}
		if(variable!=null && !"".equals(variable))
		{
			variablesReadByReadOnlyTrx.add(variable);
		}
	}
	/**
	 * A method to get map of transactions maintained by Transaction Manager.
	 * @author Ashish Walia
	 * @return Map - A map of transactions maintained by Transaction Manager.
	 */
	public Map getTrxsMap()
	{
		return trxsMap;
	}
	
	/**
	 * A method to get list of currently blocked transactions.
	 * @author Ashish Walia
	 * @return List - A list of currently blocked transactions.
	 */
	public List getBlockedTrxsList()
	{
		return blockedTrxsList;
	}
	
	/**
	 * A method to add transaction to list of currently blocked transactions
	 * @author Ashish Walia
	 * @param trxName - A string variable which holds name of transaction that needs to be added to the list of currently blocked transactions.
	 */
	public void addTrxToBlockedTrxsList(String trxName)
	{
		if(blockedTrxsList==null)
		{
			blockedTrxsList=new ArrayList();
		}
		if(trxName!=null && !"".equals(trxName) && !blockedTrxsList.contains(trxName))
		{
			blockedTrxsList.add(trxName);
		}
	}


	/**
	 * Method which creates a new transaction if it doesn't exist already.
	 * @author - Ashish Walia
	 * @param operationVO - An OperationVO object which holds all the information about any given operation.
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @throws IOException
	 */
	
	public void beginTransaction(OperationVO operationVO,BufferedWriter out)throws IOException
	{
		if(operationVO!=null)
		{
			String trxName=operationVO.getTrxName();			
			int tick=operationVO.getTick();
			boolean readOnly=false;	
			if(OperationConstants.BEGIN_READ_ONLY_TRANSACTION_OPERATION==operationVO.getTypeOfOperation())
			{
				readOnly=true;
			}
			if(trxName!=null && !"".equals(trxName))
			{
				//Check if transaction exists in map of transactions maintained by the transaction manager
				if(trxsMap!=null && !trxsMap.containsKey(trxName))
				{
					//We need to create new transaction
					Transaction trx = new Transaction(trxName,readOnly,tick);
					trxsMap.put(trxName, trx);
					if(readOnly)
					{
						//Save the values of all variables committed at sites at this tick
						readAndSaveCommittedValuesFromSite(trx);
						out.write("Read-only transaction "+trxName+" created.");
						out.newLine();
						System.out.println("Read-only transaction "+trxName+" created.");

					}
					else
					{
						out.write("Transaction "+trxName+" created.");
						out.newLine();
						System.out.println("Transaction "+trxName+" created.");
					}
				}
				else if(trxsMap!=null && trxsMap.containsKey(trxName))
				{
					out.write("Transaction "+trxName+" already exists.");
					out.newLine();
					System.out.println("Transaction "+trxName+" already exists.");
				}
			}
		}
	}
	
	/**
	 * Method which reads and saves committed values of variables when a read-only transaction is created.
	 * @author - Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 */
	public void readAndSaveCommittedValuesFromSite(Transaction trx)
	{
		if(trx!=null && dataLockManagersMap!=null)
		{
			Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
					if(dataAndLockManager!=null && dataAndLockManager.getMapOfVariablesVO()!=null)
					{
						Iterator iterator =(Iterator)dataAndLockManager.getMapOfVariablesVO().entrySet().iterator();
						while(iterator.hasNext())
						{
							Map.Entry variableVOPairs = (Map.Entry)iterator.next();
							if(variableVOPairs!=null)
							{
								VariablesVO variableVO = (VariablesVO)variableVOPairs.getValue();
								if(variableVO!=null && variableVO.getVariable()!=null && variableVO.isVariableAvailableForReadOperation())
								{
									trx.setVariablesAndValuesMapForROTrx(variableVO.getVariable(),variableVO.getValue());
								}
							}
						}
					}
				}
			}
		}
	}	
	
	/**
	 * Method which checks if a blocked transaction is in a good standing to read the variable on which it is blocked.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which the blocked transaction wants to read.
	 * @return int - An integer value representing the result of execution of the method.
	 */	
	public int shouldBlockedTrxTryAgainToReadVariable(Transaction trx, String variable)
	{
		if(trx!=null && variable!=null && !"".equals(variable) && dataLockManagersMap!=null)
		{
			Iterator itr = (Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
				DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();
				if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus())
				{
					int response=dataAndLockManager.canReadLockBeAcquiredOnVariable(trx, variable);
					if(response==TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED)
					{
						return TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED;
					}
					else if(response==TransactionConstants.ABORT_TRANSACTION)
					{
						return TransactionConstants.ABORT_TRANSACTION;
					}
				}
				}
			}
		}
		return TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED;
	}
	
	/**
	 * Method which checks if a variable can be read from any of the ten sites.
	 * @author Ashish Walia
	 * @param variable - A String variable which needs to be read from any of the ten sites.
	 * @return boolean
	 */
	public boolean isVariableAvailableToBeReadFromAnySite(String variable)
	{
		if(dataLockManagersMap!=null && !dataLockManagersMap.isEmpty() && variable!=null && !"".equals(variable))
		{
			Iterator itr = (Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();
					if(dataAndLockManager!=null && dataAndLockManager.isVariableAvailableToBeReadFromSite(variable))
					{
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Method which reads the value of variable from one of the available sites. 
	 * Available copy algorithm has been used here to read any variable.
	 * Read-only transactions use multi-version read consistency algorithm to read any variable
	 * @author Ashish Walia
	 * @param operationVO - An OperationVO object which holds all the information about any given operation
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @param tick - An integer value used to correctly maintain the time stamp at which site is accessed 
	 * @return int - An integer value representing outcome of the readVariable method's execution
	 * @throws IOException
	 */
	public int readVariable(OperationVO operationVO, BufferedWriter out,int tick)throws IOException
	{
		if(operationVO!=null)
		{
			String trxName= operationVO.getTrxName();
			String variable= operationVO.getVariable();
			int value=operationVO.getValue();
		if(trxName!=null && variable!=null && !"".equals(trxName) && !"".equals(variable))
		{
			//Check if transaction exists in map of transactions maintained by the transaction manager
			if(trxsMap!=null && trxsMap.containsKey(trxName)==false)
			{
				out.write("Transaction " + trxName +" doesn't exist. Reading of variable "+variable+" can't proceed.");
				out.newLine();
				System.out.println("Transaction " + trxName +" doesn't exist. Reading of variable "+variable+" can't proceed.");
				return TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED;
			}
			//Try to read the value of variable from lockedVariablesAndValuesMap
			else if(trxsMap!=null && trxsMap.containsKey(trxName))
			{
				Transaction trx = (Transaction)trxsMap.get(trxName);
				//If transaction is a read-only transaction, read out the committed value of variable at the transaction's birth time
				if(trx!=null && variable!=null && !"".equals(variable) && trx.isTrxReadOnly())
				{
					if(trx.getVariablesAndValuesMapForROTrx()!=null && trx.getVariablesAndValuesMapForROTrx().containsKey(variable))
					{
						if(getVariablesReadByReadOnlyTrx()!=null && getVariablesReadByReadOnlyTrx().contains(variable))
						{
							out.write("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
							out.newLine();
							System.out.println("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
							return TransactionConstants.READ_OPERATION_SUCCESSFUL_FOR_READ_ONLY_TRANSACTION;
						}
						else
						{
							//Check if the variable is available to be read from any of the site
							if(isVariableAvailableToBeReadFromAnySite(variable))
							{
								addVariableToVariablesReadByReadOnlyTrx(variable);
								out.write("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
								out.newLine();
								System.out.println("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
								return TransactionConstants.READ_OPERATION_SUCCESSFUL_FOR_READ_ONLY_TRANSACTION;
							}
							//Block the transaction
							else
							{
								out.write("Either the site(s) at which variable is present is/are down or the variable is not available for read operation at this time.");
								out.newLine();
								out.write("Transaction "+ trx.getTrxId()+" needs to be blocked.");
								out.newLine();
								out.write("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
								out.newLine();
								System.out.println("Either the site(s) at which variable is present is/are down or the variable is not available for read operation at this time.");
							    System.out.println("Transaction "+ trx.getTrxId()+" needs to be blocked.");
							    System.out.println("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
								trx.setTransactionStatus(TransactionConstants.BLOCKED_TRANSACTION_STATUS);
								if(!trx.isOperationInBlockedOperationsQueue(operationVO))
								{
									trx.addOperationsToBlockedOperationsQueue(operationVO);
								}
								addTrxToBlockedTrxsList(trxName);												
								return TransactionConstants.BLOCK_TRANSACTION;
							}
						}
					}
					else
					{
						out.write("Committed value of variable "+variable+" for read-only transaction "+trxName+" could not be found at its birth time.");
						out.newLine();
						System.out.println("Committed value of variable "+variable+" for read-only transaction "+trxName+" could not be found at its birth time.");
					}
					return TransactionConstants.READ_OPERATION_UNSUCCESSFUL_FOR_READ_ONLY_TRANSACTION;
				}
				else if(variable!=null && !"".equals(variable)&& trx!=null && trx.getLockedVariablesAndValuesMap()!=null && trx.getLockedVariablesAndValuesMap().containsKey(variable))
				{
					out.write("Value of variable "+variable+ " is "+trx.getLockedVariablesAndValuesMap().get(variable));
					out.newLine();
					System.out.println("Value of variable "+variable+ " is "+trx.getLockedVariablesAndValuesMap().get(variable));
					return 0;
				}
				else
				{
					/*
					 * 
					 * Available Copies Algorithm
					 * 
					 */

					//Step 1: Read the variable from first available site which hosts that variable
					//Iterate over dataLockManagersMap
					if(dataLockManagersMap!=null && trx!=null)
					{
						Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
						boolean readLockAcquired = false;
						while(itr.hasNext())
						{
							Map.Entry pairs = (Map.Entry)itr.next();
							if(pairs!=null)
							{
								DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
								//if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus() && dataAndLockManager.isVariableAvailableToBeReadFromSite(variable))
								if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
								{
									int response = dataAndLockManager.canReadLockBeAcquiredOnVariable(trx, variable);
									if(response==TransactionConstants.SITE_IS_DOWN)
									{
										out.write("Can't read variable "+variable +" from site "+ pairs.getKey()+" . Site "+pairs.getKey()+" is down.");
										out.newLine();
										System.out.println("Can't read variable "+variable +" from site "+ pairs.getKey()+" . Site "+pairs.getKey()+" is down.");
									}
									else if(response==TransactionConstants.VARIABLE_NOT_FOUND_AT_SITE)
									{
										out.write("Variable "+variable +" not found at site "+ pairs.getKey());
										out.newLine();
										System.out.println("Variable "+variable +" not found at site "+ pairs.getKey());
									}
									else if(response==TransactionConstants.VARIABLE_NOT_AVAILABLE_FOR_READ_OPERATION)
									{
										out.write("Variable "+variable+ "is not available for read operation at site "+pairs.getKey());
										out.newLine();
										System.out.println("Variable "+variable+ "is not available for read operation at site "+pairs.getKey());
									}
									else if(response==TransactionConstants.READ_LOCK_ALREADY_ACQUIRED)
									{
										out.write("Read lock already acquired by transaction "+trx.getTrxId()+" on variable "+variable);
										out.newLine();
										out.write("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
										out.newLine();
										System.out.println("Read lock already acquired by transaction "+trx.getTrxId()+" on variable "+variable);
										System.out.println("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
										return TransactionConstants.READ_LOCK_ACQUIRED;
									}
									else if(response==TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED)
									{
										out.write("Variable "+variable +" found at site "+ pairs.getKey());
										out.newLine();										
										out.write("Read Lock request can be granted. Proceeding to lock acquisition stage.");
										out.newLine();
										System.out.println("Variable "+variable +" found at site "+ pairs.getKey());
										System.out.println("Read Lock request can be granted. Proceeding to lock acquisition stage.");										
										//Read lock needs to be applied only at the first available site
										trx.addSiteAccessedInfoToSitesAccessedSoFarMap(dataAndLockManager.getDataAndLockManagerId(),tick);
										dataAndLockManager.acquireReadLock(trx,variable);
										out.write("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
										out.newLine();
										System.out.println("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
										trx.addReadLockedVariables(variable);
										return TransactionConstants.READ_LOCK_ACQUIRED;
									}
									else if(response==TransactionConstants.ABORT_TRANSACTION)
									{
										out.write("Variable "+variable +" found at site "+ pairs.getKey());
										out.newLine();	
										System.out.println("Variable "+variable +" found at site "+ pairs.getKey());
										dataAndLockManager.displayReasonWhyTrxNeedsToBeAborted(trx,variable,out);
										out.write("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
										out.newLine();										
										System.out.println("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
										informAllTheSitesThatTrxIsAborting(trx);
										return TransactionConstants.ABORT_TRANSACTION;
									}
									else if(response==TransactionConstants.BLOCK_TRANSACTION)
									{
										out.write("Variable "+variable +" found at site "+ pairs.getKey());
										out.newLine();	
										System.out.println("Variable "+variable +" found at site "+ pairs.getKey());										
										dataAndLockManager.displayReasonWhyTrxNeedsToBeBlocked(trx,variable,out);	
										out.write("Transaction "+ trx.getTrxId()+" needs to be blocked.");
										out.newLine();
										out.write("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
										out.newLine();																			
										System.out.println("Transaction "+ trx.getTrxId()+" needs to be blocked.");
										System.out.println("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
										trx.setTransactionStatus(TransactionConstants.BLOCKED_TRANSACTION_STATUS);
										if(!trx.isOperationInBlockedOperationsQueue(operationVO))
										{
											trx.addOperationsToBlockedOperationsQueue(operationVO);
										}										
										addTrxToBlockedTrxsList(trxName);										
										return TransactionConstants.BLOCK_TRANSACTION;
									}
									else if(response==TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED)
									{
										out.write("Write lock request can't be granted to transaction "+trx.getTrxId()+" on variable "+variable);
										out.newLine();
										System.out.println("Write lock request can't be granted to transaction "+trx.getTrxId()+" on variable "+variable);
										return TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED;
									}
								}
							}
						}
						//If we have reached at this stage, then it means either the variable is not available at all the sites or all the sites are down.
						//We need to block the transaction
						out.write("Transaction "+ trx.getTrxId()+" needs to be blocked.");
						out.newLine();
						out.write("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
						out.newLine();
						System.out.println("Transaction "+ trx.getTrxId()+" needs to be blocked.");
						System.out.println("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
						trx.setTransactionStatus(TransactionConstants.BLOCKED_TRANSACTION_STATUS);
						if(!trx.isOperationInBlockedOperationsQueue(operationVO))
						{
							trx.addOperationsToBlockedOperationsQueue(operationVO);
						}						
						addTrxToBlockedTrxsList(trxName);						
						return TransactionConstants.BLOCK_TRANSACTION;							
						//End
					}
				}
			}
		}
	}
	return TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED;
}
	/**
	 * A method which informs all the available sites that transaction is aborting. 
	 * Data and lock manager will release all the read and write locks held by the transaction on the variables at that site.
	 * At the end, transaction is removed from the map of transactions maintained by Transaction Manager
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds all the information related to a given transaction
	 */
	public void informAllTheSitesThatTrxIsAborting(Transaction trx)
	{
		if(dataLockManagersMap!=null)
		{
			Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
					if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus())
					{
						dataAndLockManager.abortTransaction(trx);
					}
				}
			}
		}
		//Now, it's time to clear the sitesAccessedSoFar Set
		trx.clearSitesAccessedSoFar();

		//Clear read and write locked variables
		trx.clearReadLockedVariablesSet();
		trx.clearWriteLockedVariablesSet();

		//Clear locked variables and value map
		trx.clearLockedVariablesAndValuesMap();	
		
		//Clear blocked operations' queue
		trx.clearBlockedOperationsQueue();
		
		//Remove transaction from list of blocked transactions' list
		if(getBlockedTrxsList()!=null && trx.getTrxId()!=null)
		{
			getBlockedTrxsList().remove(trx.getTrxId());
		}
		//Now, it's time to remove Transaction trx from the Transaction Manager trxsMap
		if(trxsMap!=null)
		{
			trxsMap.remove(trx.getTrxId());
		}		
	}
	/**
	 * Method which checks if a blocked transaction is in a good standing to write the variable on which it is blocked.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which the blocked transaction wants to read.
	 * @return int - An integer value representing the result of execution of the method.
	 */	
	public int shouldBlockedTrxTryAgainToWriteVariable(Transaction trx, String variable)
	{
		if(trx!=null && variable!=null && !"".equals(variable) && dataLockManagersMap!=null)
		{
			Iterator itr = (Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
				DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();
				if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus())
				{
					int response=dataAndLockManager.canWriteLockBeAcquiredOnVariable(trx, variable);
					if(response==TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED)
					{
						return TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED;
					}
					else if(response==TransactionConstants.ABORT_TRANSACTION)
					{
						return TransactionConstants.ABORT_TRANSACTION;
					}
				}
			}
			}
		}
		return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;
	}

	/**
	 * Method which executes queued up pending operations of blocked transactions.
	 * @author Ashish Walia
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @param tick - An integer value used in updating site accessed information which is maintained for each transaction.
	 * @throws IOException
	 */
	public void executePendingOperations(BufferedWriter out, int tick)throws IOException
	{
		if(getTrxsMap()!=null && !getTrxsMap().isEmpty() && getBlockedTrxsList()!=null && !getBlockedTrxsList().isEmpty())
		{
			Iterator iterator = (Iterator)getBlockedTrxsList().iterator();
			while(iterator.hasNext())
			{
				String trxName=(String)iterator.next();				
				if(trxName!=null && !"".equals(trxName) && getTrxsMap().containsKey(trxName))
				{
					Transaction trx = (Transaction)getTrxsMap().get(trxName);
					if(trx!=null && trx.getBlockedOperationsQueue()!=null && !trx.getBlockedOperationsQueue().isEmpty())
					{
						Iterator itr = (Iterator)trx.getBlockedOperationsQueue().iterator();
						while(trx.getBlockedOperationsQueue().peek()!=null)
						{							
							boolean stopPeeking=false;
							OperationVO operationVO = (OperationVO)trx.getBlockedOperationsQueue().peek();
							if(operationVO!=null)
							{
								int operationToBePerformed = operationVO.getTypeOfOperation();
								switch(operationToBePerformed)
								{
								case OperationConstants.COMMIT_TRANSACTION_OPERATION:	
										
									    out.write("*************************************");
										out.newLine();
										out.write(operationVO.getOperation());
										out.newLine();
										out.write("*************************************");
										out.newLine();
									    System.out.println("*************************************");
										System.out.println(operationVO.getOperation());
										System.out.println("*************************************");
										//Either the transaction will commit or abort, in both the cases we want to clear transactions' pending operations and remove transaction from
										//the list of blocked transactions
										//trx.removeOperationFromBlockedOperationsQueue(operationVO);
										if(trx.getBlockedOperationsQueue()!=null)
										{
											trx.getBlockedOperationsQueue().clear();
										}
										if(getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
										{
											iterator.remove();
										}
										//Execute the operation in a normal manner
										endTransaction(operationVO,out);
																			
									break;
								case OperationConstants.READ_OPERATION:
									if(TransactionConstants.BLOCKED_TRANSACTION_STATUS.equals(trx.getTransactionStatus()))
									{
										String variable = operationVO.getVariable();
										if(trx.isTrxReadOnly())
										{
											if(isVariableAvailableToBeReadFromAnySite(variable))
											{
												out.newLine();
												out.write("*************************************");
												out.newLine();
												out.write(operationVO.getOperation());
												out.newLine();
												out.write("*************************************");
												out.newLine();													
												out.write("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
												out.newLine();
												System.out.println();
												System.out.println("*************************************");
												System.out.println(operationVO.getOperation());
												System.out.println("*************************************");	
												System.out.println("Committed value of variable "+variable+" at read-only transaction "+trxName+"'s birth time is "+trx.getVariablesAndValuesMapForROTrx().get(variable));
												addVariableToVariablesReadByReadOnlyTrx(variable);
												//Handle blocked transaction
												trx.setTransactionStatus(TransactionConstants.ACTIVE_TRANSACTION_STATUS);
												if(trx.getBlockedOperationsQueue()!=null)
												{
													trx.getBlockedOperationsQueue().poll();
												}
												//trx.removeOperationFromBlockedOperationsQueue(operationVO);
												//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
												if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
												{
													iterator.remove();
												}
											}
											//Keep it blocked
											else
											{
												stopPeeking=true;
											}
										}
										else
										{
										int response = shouldBlockedTrxTryAgainToReadVariable(trx,variable);
										if(response==TransactionConstants.READ_LOCK_REQUEST_CAN_BE_GRANTED)
										{
											if(dataLockManagersMap!=null)
											{
												boolean readLockAcquired=false;
												Iterator itrtr = (Iterator)dataLockManagersMap.entrySet().iterator();
												while(itrtr.hasNext())
												{
													readLockAcquired=false;
													Map.Entry pairs = (Map.Entry)itrtr.next();
													if(pairs!=null)
													{
														DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();
														if(dataAndLockManager!=null && dataAndLockManager.isVariableAvailableToBeReadFromSite(variable))
														{
															out.newLine();
															out.write("*************************************");
															out.newLine();
															out.write(operationVO.getOperation());
															out.newLine();
															out.write("*************************************");
															out.newLine();
															out.write("Read Lock request can be granted. Proceeding to lock acquisition stage.");
															out.newLine();
															
															System.out.println();
															System.out.println("*************************************");
															System.out.println(operationVO.getOperation());
															System.out.println("*************************************");
															System.out.println("Read Lock request can be granted. Proceeding to lock acquisition stage.");										
															//Read lock needs to be applied only at the first available site
															trx.addSiteAccessedInfoToSitesAccessedSoFarMap(dataAndLockManager.getDataAndLockManagerId(),tick);
															dataAndLockManager.acquireReadLock(trx,variable);
															out.write("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
															out.newLine();
															System.out.println("Value of variable "+variable+" at Site "+pairs.getKey() +" is "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getValueOfVariableAtSite(variable));
															trx.addReadLockedVariables(variable);
															readLockAcquired=true;
															//Handle blocked transaction
															trx.setTransactionStatus(TransactionConstants.ACTIVE_TRANSACTION_STATUS);
															if(trx.getBlockedOperationsQueue()!=null)
															{
																trx.getBlockedOperationsQueue().poll();
															}
															//trx.removeOperationFromBlockedOperationsQueue(operationVO);
															//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
															if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
															{
																iterator.remove();
															}
														}
													}
													if(readLockAcquired)
													{
														break;
													}
												}
												
											}
										}
										else if(response==TransactionConstants.ABORT_TRANSACTION)
										{
											System.out.println("*************************************");
											System.out.println(operationVO.getOperation());
											System.out.println("*************************************");
											out.write("*************************************");
											out.newLine();
											out.write(operationVO.getOperation());
											out.newLine();
											out.write("*************************************");
											out.newLine();
											out.write("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											out.newLine();
											
											System.out.println("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											informAllTheSitesThatTrxIsAborting(trx);
											stopPeeking=true;
										}
										//Stop looking further down the queue. It's time to look at other blocked transaction's pending operations
										else
										{
											stopPeeking=true;
										}
									}
									}
									else
									{
										out.newLine();
										out.write("*************************************");
										out.newLine();
										out.write(operationVO.getOperation());
										out.newLine();
										out.write("*************************************");
										out.newLine();										
										System.out.println();
										System.out.println("*************************************");
										System.out.println(operationVO.getOperation());
										System.out.println("*************************************");
										
										int response = readVariable(operationVO,out,tick);
										if(trx.isTrxReadOnly())
										{
											if(response==TransactionConstants.READ_OPERATION_SUCCESSFUL_FOR_READ_ONLY_TRANSACTION || response==TransactionConstants.READ_OPERATION_UNSUCCESSFUL_FOR_READ_ONLY_TRANSACTION)
											{
												if(trx.getBlockedOperationsQueue()!=null)
												{
													trx.getBlockedOperationsQueue().poll();
												}
												//trx.removeOperationFromBlockedOperationsQueue(operationVO);
												//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
												if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
												{
													iterator.remove();
												}
											}
											else if(response==TransactionConstants.BLOCK_TRANSACTION)
											{
												stopPeeking=true;
											}
										}
										else
										{
										
										//Either transaction will be blocked, aborted or will be executed. 										
										
										//If transaction gets blocked, stop peeking the queue in this run. operationVO will not be removed from transaction's blocked operations queue.
										if(response==TransactionConstants.BLOCK_TRANSACTION)
										{
											stopPeeking=true;
										}
										//If read lock is acquired or read lock can't be acquired for some reason other than abort and block, we need to remove operation from the pending operations queue
										else if((response==TransactionConstants.READ_LOCK_ACQUIRED)||(response==TransactionConstants.READ_LOCK_REQUEST_CANNOT_BE_GRANTED))
										{
											if(trx.getBlockedOperationsQueue()!=null)
											{
												trx.getBlockedOperationsQueue().poll();
											}
											//trx.removeOperationFromBlockedOperationsQueue(operationVO);
											//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
											if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
											{
												iterator.remove();
											}
										}
										//If transaction gets aborted, informAllTheSitesThatTrxIsAborting(trx) method will remove the transaction from blocking transactions list maintained by 
										//the Transaction Manager and clear pending operations' queue maintained by the transaction	
									}
									}
									break;
								case OperationConstants.WRITE_OPERATION:
									if(TransactionConstants.BLOCKED_TRANSACTION_STATUS.equals(trx.getTransactionStatus()))
									{
										String variable = operationVO.getVariable();
										int value=operationVO.getValue();
										int response = shouldBlockedTrxTryAgainToWriteVariable(trx,variable);
										if(response==TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED)
										{
											out.newLine();
											out.write("*************************************");
											out.newLine();
											out.write(operationVO.getOperation());
											out.newLine();
											out.write("*************************************");
											out.newLine();
											out.write("Write lock request can be granted to Transaction "+trx.getTrxId());
											out.newLine();
											System.out.println();
											System.out.println("*************************************");
											System.out.println(operationVO.getOperation());
											System.out.println("*************************************");
											System.out.println("Write lock request can be granted to Transaction "+trx.getTrxId());									
											
											//If it's an non-replicated variable, then write lock needs to be applied only at the site hosting non-replicated variable
											if(Arrays.asList(nonReplicatedVariables).contains(variable))
											{
												int pos=Arrays.asList(nonReplicatedVariables).indexOf(variable);
												int index=siteIdForNonReplicatedVariables[pos];													
												if(dataLockManagersMap!=null && index>=1 && index<=10)
												{
													DataAndLockManager dataAndLockManager=(DataAndLockManager)dataLockManagersMap.get(index);
													if(dataAndLockManager!=null)
													{
														dataAndLockManager.acquireWriteLock(trx,variable);
														trx.addSiteAccessedInfoToSitesAccessedSoFarMap(dataAndLockManager.getDataAndLockManagerId(),tick);
														out.write("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ index);
														out.newLine();
														System.out.println("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ index);
													}
												}												
											}
											//If it's a replicated variable, then write lock needs to be applied at all the sites which are up
											else
											{
												writeLockVariableOnAllTheSites(trx,variable,tick,out);
											}											
											trx.addWriteLockedVariables(variable);
											//trxsMap.put(variable,value);
											trx.setLockedVariablesAndValuesMap(variable, value);
											//Handle blocked transaction
											trx.setTransactionStatus(TransactionConstants.ACTIVE_TRANSACTION_STATUS);
											if(trx.getBlockedOperationsQueue()!=null)
											{
												trx.getBlockedOperationsQueue().poll();
											}
											//trx.removeOperationFromBlockedOperationsQueue(operationVO);
											//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
											if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
											{
												iterator.remove();
											}
										}
										else if(response==TransactionConstants.ABORT_TRANSACTION)
										{
											out.write("*************************************");
											out.newLine();
											out.write(operationVO.getOperation());
											out.newLine();
											out.write("*************************************");
											out.newLine();
											out.write("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											out.newLine();
											System.out.println("*************************************");
											System.out.println(operationVO.getOperation());
											System.out.println("*************************************");
											System.out.println("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											informAllTheSitesThatTrxIsAborting(trx);
											stopPeeking=true;
										}
										//Stop looking further down the queue. It's time to look at other blocked transaction's pending operations
										else
										{
											stopPeeking=true;
										}
									}
									else
									{
										out.newLine();
										out.write("*************************************");
										out.newLine();
										out.write(operationVO.getOperation());
										out.newLine();
										out.write("*************************************");
										out.newLine();
										
										System.out.println();
										System.out.println("*************************************");
										System.out.println(operationVO.getOperation());
										System.out.println("*************************************");
											
										//Either transaction will be blocked, aborted or will be executed. 										
										int response = writeVariable(operationVO,out,tick);
										//If transaction gets blocked, stop peeking the queue in this run. operationVO will not be removed from transaction's blocked operations queue.
										if(response==TransactionConstants.BLOCK_TRANSACTION)
										{
											stopPeeking=true;
										}
										//If write lock is acquired or write lock can't be acquired for some reason other than abort and block, we need to remove operation from the pending operations queue
										else if((response==TransactionConstants.WRITE_LOCK_ACQUIRED)||(response==TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED))
										{
											if(trx.getBlockedOperationsQueue()!=null)
											{
												trx.getBlockedOperationsQueue().poll();
											}
											//trx.removeOperationFromBlockedOperationsQueue(operationVO);
											//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
											if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
											{
												iterator.remove();
											}
										}
										//If transaction gets aborted, informAllTheSitesThatTrxIsAborting(trx) method will remove the transaction from blocking transactions list maintained by 
										//the Transaction Manager and clear pending operations' queue maintained by the transaction										
									}
									break;
									default: 
										out.write("Unknown Operation " + operationVO.getOperation()+ " was queued. Removing it.");
										out.newLine();
										System.out.println("Unknown Operation " + operationVO.getOperation()+ " was queued. Removing it.");
										if(trx.getBlockedOperationsQueue()!=null)
										{
											trx.getBlockedOperationsQueue().poll();
										}
										//trx.removeOperationFromBlockedOperationsQueue(operationVO);
										//If all the pending operations of the transaction have been executed, remove the transaction from the blocked transactions list maintained by Transaction Manager
										if(trx.getBlockedOperationsQueue()!=null && trx.getBlockedOperationsQueue().isEmpty() && getBlockedTrxsList()!=null && getBlockedTrxsList().contains(trxName))
										{
											iterator.remove();
										}
									break;
								}								
							}
							if(stopPeeking)
							{
								break;
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * Method which describes the read and write lock information of all the available variables at all the sites which are up.
	 * @author Ashish Walia
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @throws IOException
	 */
	/**
	 * @param out
	 * @throws IOException
	 */
	public void queryState(BufferedWriter out)throws IOException
	{
		if(dataLockManagersMap!=null)
		{
			Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
					if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus() && dataAndLockManager.getSiteServicedByDataAndLockMgr().getMapOfVariablesAndValuesAtSite()!=null)
					{
						boolean displayInfo = false;
						List <VariablesVO>list = new ArrayList<VariablesVO>();
						Iterator iterator =(Iterator)dataAndLockManager.getSiteServicedByDataAndLockMgr().getMapOfVariablesAndValuesAtSite().entrySet().iterator();
						while(iterator.hasNext())
						{
							Map.Entry variablePairs = (Map.Entry)iterator.next();
							if(variablePairs!=null)
							{
								VariablesVO variableVO=(VariablesVO)variablePairs.getValue();
								if(variableVO!=null)
								{
									list.add(variableVO);
									if((variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty())||(variableVO.getTrxWriteLockingVariable()!=null && !variableVO.getTrxWriteLockingVariable().isEmpty()))
									{
										displayInfo = true;
									}
								}
							}
						}
						if(displayInfo)
						{
							out.write("----------");
							out.newLine();
							out.write("Site "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId());
							out.newLine();
							out.write("----------");
							out.newLine();
							
							System.out.println("----------");
							System.out.println("Site "+ dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId());
							System.out.println("----------");
						}
						Collections.sort(list);
						Iterator listIterator = (Iterator)list.iterator();
						while(listIterator.hasNext())
						{
							VariablesVO variableVO=(VariablesVO)listIterator.next();
							if(variableVO!=null && variableVO.getTrxsReadLockingVariable()!=null && !variableVO.getTrxsReadLockingVariable().isEmpty() )
							{
								out.write(variableVO.getVariable()+" is read-locked by   ");
								//System.out.println("Commited value of variable "+variableVO.getVariable()+" at Site "+siteId+ " is "+variableVO.getValue());
								System.out.print(variableVO.getVariable()+" is read-locked by   ");
								Iterator readLockIterator = (Iterator)variableVO.getTrxsReadLockingVariable().iterator();
								while(readLockIterator.hasNext())
								{
									Transaction trx = (Transaction)readLockIterator.next();
									if(trx!=null && trx.getTrxId()!=null && !"".equals(trx.getTrxId()))
									{
										out.write(trx.getTrxId()+"   ");
										System.out.print(trx.getTrxId()+"   ");
									}
								}
								out.newLine();
								System.out.println();
							}
							if(variableVO!=null && variableVO.getTrxWriteLockingVariable()!=null && !variableVO.getTrxWriteLockingVariable().isEmpty() )
							{
								
								//System.out.println("Commited value of variable "+variableVO.getVariable()+" at Site "+siteId+ " is "+variableVO.getValue());
								out.write(variableVO.getVariable()+" is write-locked by   ");
								System.out.print(variableVO.getVariable()+" is write-locked by   ");
								Iterator writeLockIterator = (Iterator)variableVO.getTrxWriteLockingVariable().iterator();
								while(writeLockIterator.hasNext())
								{
									Transaction trx = (Transaction)writeLockIterator.next();
									if(trx!=null && trx.getTrxId()!=null && !"".equals(trx.getTrxId()))
									{
										out.write(trx.getTrxId()+"   ");
										System.out.print(trx.getTrxId()+"   ");
									}
								}
								out.newLine();
								System.out.println();
							}
						}	
					}
				}
			}
		}
	}
	/**
	 * Method which writes the value of variable to all the available sites. 
	 * It utilizes Wait-die algorithm implemented in DataAndLockManager,to decide whether the transaction needs to be blocked or aborted.
	 * @author Ashish Walia
	 * @param operationVO - An OperationVO object which holds all the information about any given operation
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @param tick - An integer value used to correctly maintain the time stamp at which site is accessed 
	 * @return int - An integer value representing outcome of the readVariable method's execution
	 * @throws IOException
	 */
	public int writeVariable(OperationVO operationVO, BufferedWriter out, int tick)throws IOException
	{
		if(operationVO!=null)
		{
			String trxName= operationVO.getTrxName();
			String variable= operationVO.getVariable();
			int value=operationVO.getValue();
			if(trxName!=null && variable!=null && !"".equals(trxName) && !"".equals(variable))
			{
				//Check if transaction exists in map of transactions maintained by the transaction manager
				if(trxsMap!=null && trxsMap.containsKey(trxName)==false)
				{
					out.write("Transaction " + trxName +" doesn't exist. Writing of variable "+variable+" by Transaction "+ trxName+" can't proceed.");
					out.newLine();
					System.out.println("Transaction " + trxName +" doesn't exist. Writing of variable "+variable+" by Transaction "+ trxName+" can't proceed.");
					return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;
				}
				else 
				{
					if(trxsMap!=null)
					{
						Transaction trx = (Transaction)trxsMap.get(trxName);
						if(trx!=null && trx.isTrxReadOnly())
						{
							out.write("Transaction "+trxName+" is read-only transaction. Write lock request on variable "+variable+" denied to Transaction "+trxName);
							out.newLine();
							System.out.println("Transaction "+trxName+" is read-only transaction. Write lock request on variable "+variable+" denied to Transaction "+trxName);
							return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;
						}				
						else if(trx!=null && dataLockManagersMap!=null)
						{
							Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
							while(itr.hasNext())
							{
								Map.Entry pairs = (Map.Entry)itr.next();
								if(pairs!=null)
								{
									DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
									if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
									{
										int response = dataAndLockManager.canWriteLockBeAcquiredOnVariable(trx,variable);
										if(response==TransactionConstants.SITE_IS_DOWN)
										{
											out.write("Can't write variable "+variable +" to site "+ pairs.getKey()+" . Site "+pairs.getKey()+" is down.");
											out.newLine();
											System.out.println("Can't write variable "+variable +" to site "+ pairs.getKey()+" . Site "+pairs.getKey()+" is down.");
										}
										else if(response==TransactionConstants.VARIABLE_NOT_FOUND_AT_SITE)
										{
											out.write("Variable "+variable +" not found at site "+ pairs.getKey());
											out.newLine();
											System.out.println("Variable "+variable +" not found at site "+ pairs.getKey());
										}
										else if(response==TransactionConstants.WRITE_LOCK_ALREADY_ACQUIRED)
										{
											out.write("Write lock already acquired by Transaction "+trx.getTrxId()+" on variable "+variable);
											out.newLine();
											System.out.println("Write lock already acquired by Transaction "+trx.getTrxId()+" on variable "+variable);
											//trxsMap.put(variable,value);
											trx.setLockedVariablesAndValuesMap(variable, value);
											return TransactionConstants.WRITE_LOCK_ACQUIRED;
										}
										else if(response==TransactionConstants.WRITE_LOCK_REQUEST_CAN_BE_GRANTED)
										{
											
											out.write("Write lock request can be granted to Transaction "+trx.getTrxId());
											out.newLine();	
											System.out.println("Write lock request can be granted to Transaction "+trx.getTrxId());
											trx.addSiteAccessedInfoToSitesAccessedSoFarMap(dataAndLockManager.getDataAndLockManagerId(),tick);
											//If it's an non-replicated variable, then write lock needs to be applied only at the site hosting non-replicated variable
											if(Arrays.asList(nonReplicatedVariables).contains(variable))
											{
												dataAndLockManager.acquireWriteLock(trx,variable);
												out.write("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ pairs.getKey());
												out.newLine();
												System.out.println("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ pairs.getKey());
											}
											//If it's a replicated variable, then write lock needs to be applied at all the sites which are up
											else
											{
												writeLockVariableOnAllTheSites(trx,variable,tick,out);
											}
											trx.addWriteLockedVariables(variable);
											//trxsMap.put(variable,value);
											trx.setLockedVariablesAndValuesMap(variable, value);
											return TransactionConstants.WRITE_LOCK_ACQUIRED;														

										}
										else if(response==TransactionConstants.ABORT_TRANSACTION)
										{
											out.write("Variable "+variable +" found at site "+ pairs.getKey());
											out.newLine();	
											System.out.println("Variable "+variable +" found at site "+ pairs.getKey());
											dataAndLockManager.displayReasonWhyTrxNeedsToBeAborted(trx,variable,out);
											out.write("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											out.newLine();
											System.out.println("Transaction " + trx.getTrxId()+" needs to be aborted. Informing all the sites that the transaction is aborting.");
											informAllTheSitesThatTrxIsAborting(trx);
											return TransactionConstants.ABORT_TRANSACTION;
										}
										else if(response==TransactionConstants.BLOCK_TRANSACTION)
										{
												out.write("Variable "+variable +" found at site "+ pairs.getKey());
												out.newLine();	
												System.out.println("Variable "+variable +" found at site "+ pairs.getKey());										
												dataAndLockManager.displayReasonWhyTrxNeedsToBeBlocked(trx,variable,out);
												out.write("Transaction "+ trx.getTrxId()+" needs to be blocked.");
												out.newLine();
												out.write("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
												out.newLine();
											    System.out.println("Transaction "+ trx.getTrxId()+" needs to be blocked.");
											    System.out.println("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
												trx.setTransactionStatus(TransactionConstants.BLOCKED_TRANSACTION_STATUS);
												if(!trx.isOperationInBlockedOperationsQueue(operationVO))
												{
													trx.addOperationsToBlockedOperationsQueue(operationVO);
												}
												addTrxToBlockedTrxsList(trxName);												
												return TransactionConstants.BLOCK_TRANSACTION;
										}
										else if(response==TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED)
										{
											out.write("Write lock request can't be granted to transaction "+trx.getTrxId()+" on variable "+variable);
											out.newLine();
											System.out.println("Write lock request can't be granted to transaction "+trx.getTrxId()+" on variable "+variable);
											return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;
										}																				
									}
								}								
							}
							//If we have reached at this stage, then it means either the variable is not available at all the sites or all the sites are down.
							//We need to block the transaction
							out.write("Transaction "+ trx.getTrxId()+" needs to be blocked.");
							out.newLine();
							out.write("Adding operation "+operationVO.getOperation()+" to transaction "+trx.getTrxId()+"'s blocked operations queue.");
							out.newLine();
							System.out.println("Transaction "+ trx.getTrxId()+" needs to be blocked.");
							trx.setTransactionStatus(TransactionConstants.BLOCKED_TRANSACTION_STATUS);
							if(!trx.isOperationInBlockedOperationsQueue(operationVO))
							{
								trx.addOperationsToBlockedOperationsQueue(operationVO);
							}
							addTrxToBlockedTrxsList(trxName);							
							return TransactionConstants.BLOCK_TRANSACTION;							
							//End
						}
					}
				}
			}
		}
		return TransactionConstants.WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED;
	}
	/**
	 * Method which write locks the variable on all the available sites. It is internally used by the method writeVariable.
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @param variable - A String variable which needs to be write locked.
	 * @param operationVO - An OperationVO object which holds all the information about any given operation
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @param tick - An integer value used to correctly maintain the time stamp at which site is accessed 
	 * @throws IOException
	 */
	public void writeLockVariableOnAllTheSites(Transaction trx, String variable,int tick,BufferedWriter out)throws IOException
	{
		if(trx!=null && variable!=null && dataLockManagersMap!=null)
		{
			Iterator itr =(Iterator)dataLockManagersMap.entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
					DataAndLockManager dataAndLockManager=(DataAndLockManager)pairs.getValue();
					if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().isVariableAvailableOnSite(variable))
					{
						dataAndLockManager.acquireWriteLock(trx, variable);
						out.write("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ pairs.getKey());
						out.newLine();
						System.out.println("Write lock acquired by Transaction "+trx.getTrxId()+" on variable "+variable+" at Site "+ pairs.getKey());
						trx.addSiteAccessedInfoToSitesAccessedSoFarMap(dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId(),tick);
					}
				}
			}
		}

	}
	
	/**
	 * Method which checks if all the sites accessed by transaction are up at this moment or not
	 * @author Ashish Walia
	 * @param trx - A Transaction object which holds every information about any given transaction.
	 * @return boolean
	 */
	public boolean areAllSitesAccessedByTrxUp(Transaction trx)
	{
		if(trx!=null)
		{
			boolean areAllSitesUp=false;
			Iterator itr = (Iterator)trx.getSitesAccessedSoFar().entrySet().iterator();
			while(itr.hasNext())
			{
				Map.Entry pairs = (Map.Entry)itr.next();
				if(pairs!=null)
				{
				int id = ((Integer)pairs.getKey()).intValue();
				int siteAccessedAtTick = ((Integer)pairs.getValue()).intValue();
				if(dataLockManagersMap.get(id)!=null)
				{
					DataAndLockManager dataAndLockManager = (DataAndLockManager)dataLockManagersMap.get(id);
					if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus())							
					{
						if(dataAndLockManager.getSiteServicedByDataAndLockMgr().getTickWhenSiteFailed()>siteAccessedAtTick)
						{
							areAllSitesUp=false;
							return areAllSitesUp;
						}
						areAllSitesUp=true;
					}
					else
					{
						areAllSitesUp=false;
						return areAllSitesUp;
					}
				}
				}
			}
			return areAllSitesUp;
		}
		else
		{
			return false;
		}
	}

	
	/**
	 * Method to commit the transaction on all the available sites. 
	 * If all the sites are not up since the first time they were accessed by a transaction, the transaction will be aborted.
	 * @param operationVO - An OperationVO object which holds all the information about any given operation.
	 * @param out - A BufferedWriter object to write contents to output file.
	 * @throws IOException
	 */
	public void endTransaction(OperationVO operationVO, BufferedWriter out)throws IOException
	{
		if(operationVO!=null)
		{
			String trxName=operationVO.getTrxName();
			int tick=operationVO.getTick();
			if(trxName!=null && trxsMap!=null && !trxsMap.containsKey(trxName))
			{
				out.write("Either Transaction "+trxName+" doesn't exist or has been aborted or committed earlier.");
				out.newLine();
				out.write("Transaction Manager can't proceed with committing Transaction "+trxName);
				out.newLine();
				System.out.println("Either Transaction "+trxName+" doesn't exist or has been aborted or committed earlier.");
				System.out.println("Transaction Manager can't proceed with committing Transaction "+trxName);
			}

			else if(trxName!=null && trxsMap!=null && trxsMap.containsKey(trxName))
			{
				Transaction trx = (Transaction)trxsMap.get(trxName);
				if(trx!=null && trx.getSitesAccessedSoFar()!=null && !trx.getSitesAccessedSoFar().isEmpty()&& dataLockManagersMap!=null && !dataLockManagersMap.isEmpty())
				{
					boolean areAllSitesUp=true;
					out.write("Status of sites accessed by Transaction "+trxName);
					out.newLine();
					System.out.println("Status of sites accessed by Transaction "+trxName);
					//If all the sites accessed by Transaction are up then only transaction can commit, otherwise abort the transaction
					Iterator itr = (Iterator)trx.getSitesAccessedSoFar().entrySet().iterator();
					while(itr.hasNext())
					{
						Map.Entry pairs = (Map.Entry)itr.next();
						if(pairs!=null)
						{
							int id = ((Integer)pairs.getKey()).intValue();
							int siteAccessedAtTick = ((Integer)pairs.getValue()).intValue();
							if(dataLockManagersMap.get(id)!=null)
							{
								DataAndLockManager dataAndLockManager = (DataAndLockManager)dataLockManagersMap.get(id);
								if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteStatus())							
								{
									
									if(dataAndLockManager.getSiteServicedByDataAndLockMgr().getTickWhenSiteFailed()>siteAccessedAtTick)
									{
										out.write("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - UP, FAILED AT TICK = " + dataAndLockManager.getSiteServicedByDataAndLockMgr().getTickWhenSiteFailed()+", FIRST ACCESSED AT TICK = "+siteAccessedAtTick+"    ");
										System.out.print("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - UP, FAILED AT TICK = " + dataAndLockManager.getSiteServicedByDataAndLockMgr().getTickWhenSiteFailed()+", FIRST ACCESSED AT TICK = "+siteAccessedAtTick+"    ");
										areAllSitesUp=(areAllSitesUp && false);
									}
									else
									{
										out.write("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - UP    ");
										System.out.print("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - UP    ");
										areAllSitesUp=(areAllSitesUp && true);
									}
								}
								else
								{
									out.write("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - DOWN    ");
									System.out.print("Site "+dataAndLockManager.getSiteServicedByDataAndLockMgr().getSiteId()+" - DOWN    ");
									areAllSitesUp=(areAllSitesUp && false);
	
								}
							}
						}
					}
					out.newLine();
					System.out.println();
					//Commit transaction only if all the sites accessed by transaction are up
					if(areAllSitesUp)
					{
						int sumOfResponse=0;
						Iterator iterator = (Iterator)dataLockManagersMap.entrySet().iterator();
						while(iterator.hasNext())
						{
							Map.Entry pairs = (Map.Entry)iterator.next();
							if(pairs!=null)
							{
								DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();
								//Send information to all the sites that the transaction trx is committing
								if(dataAndLockManager!=null)
								{
									int response = dataAndLockManager.commitValuesForVariablesLockedByTransaction(trx);
									sumOfResponse+=response;
									if(response>0)
									{
										out.write("Transaction "+trx.getTrxId()+" got commited at Site "+dataAndLockManager.getDataAndLockManagerId());
										out.newLine();
										System.out.println("Transaction "+trx.getTrxId()+" got commited at Site "+dataAndLockManager.getDataAndLockManagerId());
									}
								}

							}
						}
						
							out.write("Transaction "+trx.getTrxId()+" ended successfully.");
							out.newLine();
							System.out.println("Transaction "+trx.getTrxId()+" ended successfully.");
						

						//Step 1 : Now removing the read locked and write variables from the committed transaction
						trx.clearWriteLockedVariablesSet();
						trx.clearReadLockedVariablesSet();
						//Step 2: Clear variables and values map
						trx.clearLockedVariablesAndValuesMap();	
						//Step 3: Finally remove the transaction trx from trxsMap maintained by TransactionManager
						trxsMap.remove(trx.getTrxId());

					}//Abort the transaction
					else
					{
						out.write("Transaction "+trxName+" can't be committed as all the sites are not up since the first time they were accessed. Aborting the transaction.");
						out.newLine();
						System.out.println("Transaction "+trxName+" can't be committed as all the sites are not up since the first time they were accessed. Aborting the transaction.");
						informAllTheSitesThatTrxIsAborting(trx);
					}
				}
				else
				{
					if(trx!=null)
					{
						out.write("Transaction "+trxName+ " ended successfuly.");
						out.newLine();
						//If trx is read only or trx has not read locked or write locked any variable
						System.out.println("Transaction "+trxName+ " ended successfuly.");
						trxsMap.remove(trx.getTrxId());
					}
				}
			}
		}
	}

}
