package transaction;

/**
 * A file maintaining constants useful in acquiring read and write locks.
 * @author Ashish Walia
 *
 */

public  class TransactionConstants {	
public static String ACTIVE_TRANSACTION_STATUS="ACTIVE";
public static String BLOCKED_TRANSACTION_STATUS="BLOCKED";
public static String ABORTED_TRANSACTION_STATUS="ABORTED";
public static final int SITE_IS_DOWN = 400;
public static final int READ_LOCK_ALREADY_ACQUIRED=401;
public static final int WRITE_LOCK_ALREADY_ACQUIRED=402;
public static final int VARIABLE_NOT_FOUND_AT_SITE = 404;
public static final int VARIABLE_NOT_AVAILABLE_FOR_READ_OPERATION=405;
public static final int READ_LOCK_REQUEST_CAN_BE_GRANTED=1;
public static final int READ_LOCK_REQUEST_CANNOT_BE_GRANTED=0;
public static final int WRITE_LOCK_REQUEST_CAN_BE_GRANTED=1;
public static final int WRITE_LOCK_REQUEST_CANNOT_BE_GRANTED=0;
public static final int WRITE_LOCK_ACQUIRED=1;
public static final int READ_LOCK_ACQUIRED=1;
public static final int ABORT_TRANSACTION=500;
public static final int BLOCK_TRANSACTION=501;
public static final int READ_OPERATION_SUCCESSFUL_FOR_READ_ONLY_TRANSACTION=600;
public static final int READ_OPERATION_UNSUCCESSFUL_FOR_READ_ONLY_TRANSACTION=601;
}
