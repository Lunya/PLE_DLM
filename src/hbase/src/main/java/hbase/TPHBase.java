//=====================================================================
/**
 * Squelette minimal d'une application HBase 0.99.1
 * A exporter dans un jar sans les librairies externes
 * Il faut initialiser la variable d'environement HADOOP_CLASSPATH
 * Il faut utiliser la commande hbase 
 * A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
 */
package hbase;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
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

import com.github.davidmoten.geo.GeoHash;


public class TPHBase {

	public static class HBaseProg extends Configured implements Tool {
		public class CreateDatabase {
			private final byte[] TABLE_NAME = Bytes.toBytes("dlm");
			
			private final String LEVEL_PREFIX_ROW = "l";
			private final byte[] HEIGHT_FAMILY = Bytes.toBytes("h");
			
			private final byte[] LATITUDE_COL = Bytes.toBytes("la");
			private final byte[] LONGITUDE_COL = Bytes.toBytes("lo");
			private final byte[] ZOOM_COL = Bytes.toBytes("z");
			private final byte[] HEIGHT_COL = Bytes.toBytes("h");
			
			private int levels;
			private int imageSize;
			private int geohashPrecision;
			private Connection connection;
			
			public CreateDatabase(int levels, int imageSize, int geohashPrecision) throws IOException {
				this.levels = levels;
				this.imageSize = imageSize;
				this.geohashPrecision = geohashPrecision;
				
				connection = ConnectionFactory.createConnection(getConf());
				createTable();
				//populateTable(connection);
			}
			
			private void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
				if (admin.tableExists(table.getTableName())) {
					admin.disableTable(table.getTableName());
					admin.deleteTable(table.getTableName());
				}
				admin.createTable(table);
			}
			
			private void createTable() {
				try {
					final Admin admin = connection.getAdmin();
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
			
			private void populateTable(Connection conn) throws IOException {
				Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
				
				for (int i = 0; i < 10; ++i) {
					Put row = new Put(Bytes.toBytes(LEVEL_PREFIX_ROW + Integer.toString(i)));
					row.addColumn(HEIGHT_FAMILY, LATITUDE_COL, Bytes.toBytes(i*2));
					row.addColumn(HEIGHT_FAMILY, LONGITUDE_COL, Bytes.toBytes(i+2));
					row.addColumn(HEIGHT_FAMILY, ZOOM_COL, Bytes.toBytes(i%3));
					
					ByteBuffer bb = ByteBuffer.allocate(2 * imageSize * imageSize);
					for (int y = 0; y < imageSize; ++y)
						for (int x = 0; x < imageSize; ++x) {
							int value = y * imageSize + x;
							bb.putShort((short)(value - Short.MAX_VALUE));
						}
					row.addColumn(HEIGHT_FAMILY, HEIGHT_COL, bb.array());
					table.put(row);
				}
			}
			
			public boolean addImage(double latitude, double longitude, int level) {
				boolean inserted = false;
				level = Math.min(Math.max(level, 0), this.levels);
				
				// Put row = new Put(Bytes.toBytes(LEVEL_PREFIX_ROW + Integer.toString(level)));
				Put row = new Put(Bytes.toBytes(GeoHash.encodeHash(latitude, longitude, geohashPrecision)));
				
				row.addColumn(HEIGHT_FAMILY, LATITUDE_COL, Bytes.toBytes((float)latitude));
				row.addColumn(HEIGHT_FAMILY, LONGITUDE_COL, Bytes.toBytes((float)longitude));
				Random rand = new Random();
				ByteBuffer bb = ByteBuffer.allocate(2 * imageSize * imageSize);
				for (int y = 0; y < imageSize; ++y)
					for (int x = 0; x < imageSize; ++x) {
						int value = y * imageSize + x;
						value = Math.getExponent(rand.nextDouble() * 9.0); // random number between 0 and 8000
						bb.putShort((short)(value - Short.MAX_VALUE));
					}
				row.addColumn(HEIGHT_FAMILY, HEIGHT_COL, bb.array());
				try {
					connection.getTable(TableName.valueOf(TABLE_NAME)).put(row);
					inserted = false;
					System.out.println("Row added");
				} catch (Exception e) {
					e.printStackTrace();
				}
				return inserted;
			}
		};

		public int run(String[] args) throws IOException {
			CreateDatabase cd = new CreateDatabase(5, 256, 8);
			cd.addImage(-90.0, -180.0, 0);
			cd.addImage(-90.0, -180.0, 1);
			cd.addImage(-90.0, -180.0, 2);
			cd.addImage(0.0, -0.0, 0);
			cd.addImage(1.0, 1.0, 0);
			return 0;
		}

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TPHBase.HBaseProg(), args);
		System.exit(exitCode);
	}
}

