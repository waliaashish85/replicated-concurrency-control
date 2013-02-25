import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.io.*;
import transaction.*;
import operation.OperationConstants;
import operation.OperationVO;

import site.DataAndLockManager;
import site.VariablesVO;

/**
 * Helper class which assists main driver RepCRec.java file in parsing the input file, initializing sites, data and lock manager and transaction manager, and executing operations.
 * @author Ashish Walia
 *
 */
public class RepCRecHelper {
	
	/**
	 * A method to initialize site with appropriate variables and values
	 * @author Ashish Walia
	 * @param arr - An array of Strings containing variables
	 * @param value - An array of integers containing values
	 * @param mapOfVariablesAndValues - A map of variables and values
	 */
	public void initializeSite(String arr[],int value[],Map mapOfVariablesAndValues)
	{
		for(int i=0;i<arr.length;i++)
		{
			VariablesVO variableVO = new VariablesVO(arr[i],value[i]);
			mapOfVariablesAndValues.put(arr[i],variableVO);
			//variablesAtSite.add(arr[i]);
		}
	}
	/**
	 * A method to parse input file and store operations as list of OperationVO objects
	 * @author Ashish Walia
	 * @param bufferedReader - A BufferedReader object to read the input file.
	 * @param operations - A list of OperationVO objects
	 * @throws IOException
	 */
	public void parseInputFile(BufferedReader bufferedReader, List operations)throws IOException
	{
		
		if(bufferedReader!=null)
		{
			String line = null;
			int tick=0;
		    while((line = bufferedReader.readLine())!=null)
		    {
		    	tick++;
		    	parseLineAndConvertToOperation(line,tick,operations);		    	
		    }
		    bufferedReader.close();
		}
	}
	
	public void addOperationToList(String operation,int tick, List operations)
	{
		if(operation!=null && !"".equals(operation) && operation.endsWith(")"))
		{
			String trxName="";
			String variable="";
			int value=0;
			String operationString=operation;
			int bracesStartIndex = operationString.indexOf("(");
			int bracesEndIndex = operationString.indexOf(")");
			int indexOfFirstComma = operationString.indexOf(",");
			int indexOfLastComma= operationString.lastIndexOf(",");
			int numberOfCommas=countNumberOfInstancesOfCharInString(operationString,',');
			if(operationString.startsWith("begin("))
			{
				
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex > (bracesStartIndex+1)))
				{
					trxName=operationString.substring(bracesStartIndex+1, bracesEndIndex);
					OperationVO operationVO = new OperationVO(operationString,OperationConstants.BEGIN_TRANSACTION_OPERATION,tick);
					operationVO.setTrxName(trxName);
					if(operations!=null)
					{
						System.out.println(operationVO.getOperation());
						operations.add(operationVO);
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("end("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex > (bracesStartIndex+1)))
				{
					trxName=operationString.substring(bracesStartIndex+1, bracesEndIndex);
					OperationVO operationVO = new OperationVO(operationString,OperationConstants.COMMIT_TRANSACTION_OPERATION,tick);
					operationVO.setTrxName(trxName);
					if(operations!=null)
					{
						System.out.println(operationVO.getOperation());
						operations.add(operationVO);
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("beginRO("))
			{
				
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex > (bracesStartIndex+1)))
				{
					trxName=operationString.substring(bracesStartIndex+1, bracesEndIndex);
					OperationVO operationVO = new OperationVO(operationString,OperationConstants.BEGIN_READ_ONLY_TRANSACTION_OPERATION,tick);
					operationVO.setTrxName(trxName);
					if(operations!=null)
					{
						System.out.println(operationVO.getOperation());
						operations.add(operationVO);
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("W("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (indexOfFirstComma>0) && (indexOfLastComma>0) && (bracesEndIndex > (bracesStartIndex+1)) && (numberOfCommas==2) && (indexOfFirstComma>((bracesStartIndex+1))) && (indexOfLastComma>(indexOfFirstComma+1)) &&(bracesEndIndex>(indexOfLastComma+1)))
				{
					trxName=operationString.substring(bracesStartIndex+1, indexOfFirstComma);
					variable=operationString.substring(indexOfFirstComma+1,indexOfLastComma);
					try
					{
						value=Integer.parseInt(operationString.substring(indexOfLastComma+1,bracesEndIndex));
						OperationVO operationVO = new OperationVO(operationString,OperationConstants.WRITE_OPERATION,tick);
						operationVO.setTrxName(trxName);
						operationVO.setVariable(variable);
						operationVO.setValue(value);
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}
					}
					catch(NumberFormatException numberFormatException)
					{
						System.out.println("Invalid I/P "+ operationString+" encountered.");
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("R("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (indexOfFirstComma>0) && (bracesEndIndex > (bracesStartIndex+1)) && (numberOfCommas==1) && (indexOfFirstComma>((bracesStartIndex+1))) &&(bracesEndIndex>(indexOfFirstComma+1)))
				{
					trxName=operationString.substring(bracesStartIndex+1, indexOfFirstComma);
					variable=operationString.substring(indexOfFirstComma+1,bracesEndIndex);
					
						OperationVO operationVO = new OperationVO(operationString,OperationConstants.READ_OPERATION,tick);
						operationVO.setTrxName(trxName);
						operationVO.setVariable(variable);
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}									
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("fail("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex > (bracesStartIndex+1)))
				{
					try
					{
						value=Integer.parseInt(operationString.substring(bracesStartIndex+1,bracesEndIndex));
						OperationVO operationVO = new OperationVO(operationString,OperationConstants.FAIL_SITE_OPERATION,tick);										
						operationVO.setValue(value);
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}
					}
					catch(NumberFormatException numberFormatException)
					{
						System.out.println("Invalid I/P "+ operationString+" encountered.");
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("recover("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex > (bracesStartIndex+1)))
				{
					try
					{
						value=Integer.parseInt(operationString.substring(bracesStartIndex+1,bracesEndIndex));
						OperationVO operationVO = new OperationVO(operationString,OperationConstants.RECOVER_SITE_OPERATION,tick);										
						operationVO.setValue(value);
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}
					}
					catch(NumberFormatException numberFormatException)
					{
						System.out.println("Invalid I/P "+ operationString+" encountered.");
					}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else if(operationString.startsWith("dump("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0))
				{
					OperationVO operationVO =null;
					if(bracesEndIndex==(bracesStartIndex+1))
					{
						operationVO = new OperationVO(operationString,OperationConstants.DUMP_ALL_OPERATION,tick);							
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}
					}
					else if(bracesEndIndex>(bracesStartIndex+1))
					{
						try
						{
							//dump(1)
							value=Integer.parseInt(operationString.substring(bracesStartIndex+1,bracesEndIndex));
							operationVO = new OperationVO(operationString,OperationConstants.DUMP_SITE_OPERATION,tick);										
							operationVO.setValue(value);
								if(operations!=null)
								{
									System.out.println(operationVO.getOperation());
									operations.add(operationVO);
								}
						}
						catch(NumberFormatException numberFormatException)
						{
							//dump(x1)
							variable=operationString.substring(bracesStartIndex+1,bracesEndIndex);
							operationVO = new OperationVO(operationString,OperationConstants.DUMP_VARIABLE_OPERATION,tick);										
							operationVO.setVariable(variable);
								if(operations!=null)
								{
									System.out.println(operationVO.getOperation());
									operations.add(operationVO);
								}
						}
					}
				}
			}
			else if(operationString.startsWith("querystate("))
			{
				if((bracesEndIndex>0) && (bracesStartIndex>0) && (bracesEndIndex ==(bracesStartIndex+1)))
				{										
						OperationVO operationVO = new OperationVO(operationString,OperationConstants.QUERY_STATE_OPERATION,tick);
							if(operations!=null)
							{
								System.out.println(operationVO.getOperation());
								operations.add(operationVO);
							}
				}
				else
				{
					System.out.println("Invalid I/P "+ operationString+" encountered.");
				}
			}
			else
			{
				System.out.println("Invalid I/P "+ operationString+" encountered.");
			}
		}
		else
		{
			System.out.println("Invalid I/P "+ operation+" encountered.");
		}
	}
	public void parseLineAndConvertToOperation(String line,int tick, List operations)
	{
		if(line!=null)
		{
			line = removeSpaces(line);
			if(line.endsWith(";"))
			{
				line = line.substring(0,line.length()-1);
			}
			else if(line.startsWith(";"))
			{
				line = line.substring(1,line.length());
			}
			if(line.contains(";"))
			{
				String operationsArray[]=line.split(";");
				if(operationsArray!=null && operationsArray.length>0)
				{
					for(int i = 0;i<operationsArray.length;i++)
					{
						addOperationToList(operationsArray[i], tick, operations);
					}
				}
			}
			//A single non co-temporous operation encountered
			else
			{
				addOperationToList(line, tick, operations);
			}
		}
	}
	public int countNumberOfInstancesOfCharInString(String str,char chr)
	{
		int result=0;
		if(str!=null&& !"".equals(str))
		{
			for(int i = 0; i < str.length(); i++)
	        {
	            if(str.charAt(i)==chr)
	            {
	            	result++;
	            }
	        }
		}
		return result;
	}
	public String removeSpaces(String line)
    {
        String returnValue = "";
        for(int i = 0; i < line.length(); i++)
        {
            if(line.charAt(i) != ' ')
            {
            	returnValue += line.charAt(i);
            }
        }
        return returnValue;
    }
	
	/**
	 * A method to execute all the operations read from the input file
	 * @author Ashish Walia
	 * @param transactionManager - An instance of TransactionManager
	 * @param dataAndLockManagersMap - A map containing all the instances of data and lock managers
	 * @param operations - A list of operations that need to be executed.
	 * @param out - A BufferedWriter object to write contents in the output file
	 * @throws IOException
	 */
	public void executeOperations(TransactionManager transactionManager,Map dataAndLockManagersMap ,List operations,BufferedWriter out)throws IOException
	{
		//Iterating List of operations
		if(operations!=null && transactionManager!=null && dataAndLockManagersMap!=null)
		{
						
			Iterator itr = (Iterator)operations.iterator();
			int previousTick=0;
			while(itr.hasNext())
			{
				OperationVO operationVO = (OperationVO)itr.next();
				if(operationVO!=null)
				{				
					int operationToBePerformed = operationVO.getTypeOfOperation();
					int currentTick = operationVO.getTick();
					if(previousTick!=currentTick)
					{
						//Execute pending operations
						transactionManager.executePendingOperations(out,currentTick);
						previousTick=currentTick;
					}
					out.newLine();
					out.write("*************************************");
					out.newLine();
					out.write(operationVO.getOperation());
					out.newLine();
					out.write("*************************************");
					out.newLine();
					System.out.println("");
					System.out.println("*************************************");
					System.out.println(operationVO.getOperation());
					System.out.println("*************************************");
					switch(operationToBePerformed)
					{
					case OperationConstants.BEGIN_TRANSACTION_OPERATION : 
						if(operationVO.getTrxName()!=null && !"".equals(operationVO.getTrxName()))
						{
							//transactionManager.beginTransaction(operationVO.getTrxName(), false, operationVO.getTick());
							transactionManager.beginTransaction(operationVO,out);
						}
						break;
					case OperationConstants.BEGIN_READ_ONLY_TRANSACTION_OPERATION:
						if(operationVO.getTrxName()!=null && !"".equals(operationVO.getTrxName()))
						{
							transactionManager.beginTransaction(operationVO,out);
						}
						break;
					case OperationConstants.COMMIT_TRANSACTION_OPERATION:
						if(operationVO.getTrxName()!=null && !"".equals(operationVO.getTrxName()))
						{
							if(transactionManager.getBlockedTrxsList()!=null && transactionManager.getBlockedTrxsList().contains(operationVO.getTrxName()))
							{
									System.out.println("Transaction "+operationVO.getTrxName()+" is currently blocked and can't be committed.");									
									if(transactionManager.getTrxsMap()!=null && transactionManager.getTrxsMap().containsKey(operationVO.getTrxName()))
									{
										Transaction trx = (Transaction)transactionManager.getTrxsMap().get(operationVO.getTrxName());
										if(trx!=null)
										{
											out.write("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											out.newLine();
											System.out.println("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											trx.addOperationsToBlockedOperationsQueue(operationVO);
										}										
									}
							}
							else
							{
								transactionManager.endTransaction(operationVO,out);
							}
						}
						break;
					case OperationConstants.READ_OPERATION:
						if(operationVO.getTrxName()!=null && !"".equals(operationVO.getTrxName()) && operationVO.getVariable()!=null && !"".equals(operationVO.getVariable()))
						{
							
							if(transactionManager.getBlockedTrxsList()!=null && transactionManager.getBlockedTrxsList().contains(operationVO.getTrxName()))
							{
								out.write("Transaction "+operationVO.getTrxName()+" is currently blocked.");
								out.newLine();
								System.out.println("Transaction "+operationVO.getTrxName()+" is currently blocked.");									
									if(transactionManager.getTrxsMap()!=null && transactionManager.getTrxsMap().containsKey(operationVO.getTrxName()))
									{
										Transaction trx = (Transaction)transactionManager.getTrxsMap().get(operationVO.getTrxName());
										if(trx!=null)
										{
											out.write("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											out.newLine();
											System.out.println("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											trx.addOperationsToBlockedOperationsQueue(operationVO);
										}										
									}
							}
							else
							{
								transactionManager.readVariable(operationVO,out,currentTick);
							}
						}
						break;
					case OperationConstants.WRITE_OPERATION:
						if(operationVO.getTrxName()!=null && !"".equals(operationVO.getTrxName()) && operationVO.getVariable()!=null && !"".equals(operationVO.getVariable()))
						{
							if(transactionManager.getBlockedTrxsList()!=null && transactionManager.getBlockedTrxsList().contains(operationVO.getTrxName()))
							{
								out.write("Transaction "+operationVO.getTrxName()+" is currently blocked.");
								out.newLine();
								System.out.println("Transaction "+operationVO.getTrxName()+" is currently blocked.");									
									if(transactionManager.getTrxsMap()!=null && transactionManager.getTrxsMap().containsKey(operationVO.getTrxName()))
									{
										Transaction trx = (Transaction)transactionManager.getTrxsMap().get(operationVO.getTrxName());
										if(trx!=null)
										{
											out.write("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											out.newLine();
											System.out.println("Adding "+operationVO.getOperation()+" to Transaction "+operationVO.getTrxName()+"'s blocked operations' queue.");
											trx.addOperationsToBlockedOperationsQueue(operationVO);
										}										
									}
							}
							else
							{
								transactionManager.writeVariable(operationVO,out,currentTick);
							}
						}
						break;
						
					case OperationConstants.DUMP_ALL_OPERATION:
						if(dataAndLockManagersMap!=null)
						{
							Iterator iterator = (Iterator)dataAndLockManagersMap.entrySet().iterator();
							while(iterator.hasNext())
							{
								Map.Entry pairs = (Map.Entry)iterator.next();
								if(pairs!=null)
								{
									DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();									
									if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
									{
										dataAndLockManager.getSiteServicedByDataAndLockMgr().dump(out);
									}
								}
						}
						}
						break;
					case OperationConstants.DUMP_SITE_OPERATION:
						if(dataAndLockManagersMap!=null)
						{
							
									DataAndLockManager dataAndLockManager = (DataAndLockManager)dataAndLockManagersMap.get(operationVO.getValue());									
									if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
									{
										dataAndLockManager.getSiteServicedByDataAndLockMgr().dump(out);
									}

								
						}
						break;
					case OperationConstants.DUMP_VARIABLE_OPERATION:
						if(dataAndLockManagersMap!=null && operationVO.getVariable()!=null)
						{
							Iterator iterator = (Iterator)dataAndLockManagersMap.entrySet().iterator();
							while(iterator.hasNext())
							{
								Map.Entry pairs = (Map.Entry)iterator.next();
								if(pairs!=null)
								{
									DataAndLockManager dataAndLockManager = (DataAndLockManager)pairs.getValue();									
									if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
									{
										dataAndLockManager.getSiteServicedByDataAndLockMgr().dump(operationVO.getVariable(),out);
									}

								}
						}
						}
						break;
					case OperationConstants.FAIL_SITE_OPERATION:
						if(operationVO.getValue()>10 || operationVO.getValue()<0)
						{
							System.out.println("Invalid Site"+operationVO.getValue());
							out.write("Invalid Site"+operationVO.getValue());
							out.newLine();
						}
						else if(dataAndLockManagersMap!=null)
						{
									DataAndLockManager dataAndLockManager = (DataAndLockManager)dataAndLockManagersMap.get(operationVO.getValue());									
									if(dataAndLockManager!=null && dataAndLockManager.getSiteServicedByDataAndLockMgr()!=null)
									{
										dataAndLockManager.failSite(operationVO.getTick(),out);
									}								
						}
						break;
					case OperationConstants.RECOVER_SITE_OPERATION:
						if(operationVO.getValue()>10 || operationVO.getValue()<0)
						{
							out.write("Invalid Site"+operationVO.getValue());
							out.newLine();
							System.out.println("Invalid Site"+operationVO.getValue());
						}
						else if(dataAndLockManagersMap!=null)
						{
									DataAndLockManager dataAndLockManager = (DataAndLockManager)dataAndLockManagersMap.get(operationVO.getValue());									
									if(dataAndLockManager!=null)
									{
										dataAndLockManager.recoverSite(out);
									}								
						}
						break;
					case OperationConstants.QUERY_STATE_OPERATION:
						transactionManager.queryState(out);
						break;
						default:
							out.write("Unrecognised operation "+ operationVO.getOperation()+" encountered.");
                            out.newLine();						
							System.out.println("Unrecognised operation "+ operationVO.getOperation()+" encountered.");
						break;
						
					}
				}
			}
		}		
	}

	
}
