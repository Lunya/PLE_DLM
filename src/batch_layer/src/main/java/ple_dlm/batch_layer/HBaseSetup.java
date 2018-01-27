package ple_dlm.batch_layer;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSetup {
	public static final byte[] HEIGHT_FAMILY = Bytes.toBytes("h");
	public static final byte[] INTERESTING_POINT_FAMILY = Bytes.toBytes("h");
	
	public static final byte[] LATITUDE_COL = Bytes.toBytes("a");
	public static final byte[] LONGITUDE_COL = Bytes.toBytes("o");
	public static final byte[] ZOOM_COL = Bytes.toBytes("z");
	public static final byte[] HEIGHT_COL = Bytes.toBytes("h");
	
	private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	
	private static void createHeightColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor(HEIGHT_FAMILY);
		table.addFamily(columnFamily);
	}
	
	private static void createInterestingPointColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor(INTERESTING_POINT_FAMILY);
		table.addFamily(columnFamily);
	}
	
	public static void setup(Connection connection, String tableName) throws IOException {
		final Admin admin = connection.getAdmin();
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
		
		createHeightColumnFamily(table);
		createInterestingPointColumnFamily(table);
		
		createOrOverwrite(admin, table);
	}
}
