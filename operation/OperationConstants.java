package operation;
/**
 * A file maintaining constants useful for uniquely identifying type of operation to be performed.
 * @author Ashish Walia
 */
public class OperationConstants {
	public static final int BEGIN_TRANSACTION_OPERATION = 1;
	public static final int BEGIN_READ_ONLY_TRANSACTION_OPERATION=2;
	public static final int READ_OPERATION=3;
	public static final int WRITE_OPERATION=4;
	public static final int DUMP_ALL_OPERATION=5;
	public static final int DUMP_SITE_OPERATION=6;
	public static final int DUMP_VARIABLE_OPERATION=7;
	public static final int FAIL_SITE_OPERATION=8;
	public static final int RECOVER_SITE_OPERATION=9;
	public static final int COMMIT_TRANSACTION_OPERATION=10;
	public static final int QUERY_STATE_OPERATION=11;

}
