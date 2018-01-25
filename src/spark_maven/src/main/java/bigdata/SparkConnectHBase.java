package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SparkConnectHBase {
	public static void main(String[] args) throws Exception {
		// Définition de la rangée à récupérer
		final byte[] ROW = Bytes.toBytes("00000000");
		Get get = new Get(ROW);
		
		// Configuration
		Configuration hbconf = null;
		hbconf = HBaseConfiguration.create();
		hbconf.set("hbase.zookeeper.quorum", "10.0.8.3");
		hbconf.set("hbase.zookeeper.property.clientPort", "2181");

		// Connexion
		Connection hbc = ConnectionFactory.createConnection(hbconf);
		System.out.println("Connexion HBase");
		
		// Définition de la table
		Table table = hbc.getTable(TableName.valueOf("dlm"));
		table.get(get);
		
		// Nettoyage
		table.close();
		hbc.close();
	}
}
