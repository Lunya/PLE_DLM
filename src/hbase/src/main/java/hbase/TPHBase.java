//=====================================================================
/**
 * Squelette minimal d'une application HBase 0.99.1
 * A exporter dans un jar sans les librairies externes
 * Il faut initialiser la variable d'environement HADOOP_CLASSPATH
 * Il faut utiliser la commande hbase 
 * A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
 */
package hbase;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.beanutils.converters.BigIntegerConverter;
import org.apache.commons.beanutils.converters.ByteArrayConverter;
import org.apache.commons.beanutils.converters.ByteConverter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TPHBase {

	public static class HBaseProg extends Configured implements Tool {
		private static final byte[] TABLE_NAME = Bytes.toBytes("DLM");
		
		private static final byte[] HEIGHT_FAMILY = Bytes.toBytes("h");
		
		private static final byte[] LATITUDE_COL = Bytes.toBytes("la");
		private static final byte[] LONGITUDE_COL = Bytes.toBytes("lo");
		private static final byte[] ZOOM_COL = Bytes.toBytes("z");
		private static final byte[] HEIGHT_COL = Bytes.toBytes("h");

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				
				HColumnDescriptor heightFamily = new HColumnDescriptor(HEIGHT_FAMILY);
				
				tableDescriptor.addFamily(heightFamily);
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public int run(String[] args) throws IOException {
			Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Put row = new Put(Bytes.toBytes("row1"));
			row.addColumn(HEIGHT_FAMILY, LATITUDE_COL, Bytes.toBytes(40));
			row.addColumn(HEIGHT_FAMILY, LONGITUDE_COL, Bytes.toBytes(30));
			row.addColumn(HEIGHT_FAMILY, ZOOM_COL, Bytes.toBytes(3));
			int imgSize = 256;
			ByteBuffer bb = ByteBuffer.allocate(2 * imgSize * imgSize);
			for (int x = 0; x < imgSize*imgSize; x++) {
				bb.putShort((short)(x - 0xFFFF));
			}
			FileUtils.writeByteArrayToFile(new File("binfile"), bb.array());
			row.addColumn(HEIGHT_FAMILY, HEIGHT_COL, bb.array());
			table.put(row);
			return 0;
		}

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TPHBase.HBaseProg(), args);
		System.exit(exitCode);
	}
}

