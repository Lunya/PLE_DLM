package ple_dlm.batch_layer;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseSetup {
	public static final String HEIGHT_FAMILY = "h";
	public static final String INTERESTING_POINT_FAMILY = "h";
	
	public static final String LATITUDE_COL = "a";
	public static final String LONGITUDE_COL = "o";
	public static final String ZOOM_COL = "z";
	public static final String HEIGHT_COL = "h";
	
	private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	
	private static void createHeightColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor(HEIGHT_FAMILY.getBytes());
		table.addFamily(columnFamily);
	}
	
	private static void createInterestingPointColumnFamily(HTableDescriptor table) {
		HColumnDescriptor columnFamily = new HColumnDescriptor(INTERESTING_POINT_FAMILY.getBytes());
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
