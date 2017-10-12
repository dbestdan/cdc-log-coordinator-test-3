import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;

public class Server implements Config {

	public static void main(String[] args) {
		long sessionEndTime = 0L;
		try {
			Properties prop = new Properties();
			try {
				prop.load(new FileInputStream(System.getProperty("prop")));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			String user = prop.getProperty("user");
			String password = prop.getProperty("password");
			String url = prop.getProperty("url");

			Properties props = new Properties();
			PGProperty.USER.set(props, user);
			PGProperty.PASSWORD.set(props, password);
			PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
			PGProperty.REPLICATION.set(props, "database");
			PGProperty.PREFER_QUERY_MODE.set(props, "simple");

			Connection conn = DriverManager.getConnection(url, props);
			PGConnection replConnection = conn.unwrap(PGConnection.class);

			// replConnection.getReplicationAPI()
			// .createReplicationSlot()
			// .logical()
			// .withSlotName("demo_logical_slot")
			// .withOutputPlugin("test_decoding")
			// .make();

			// some changes after create replication slot to demonstrate receive
			// it
			Connection con = getConnection();

			String replicationSlot = System.getProperty("replicationSlot");

			PGReplicationStream stream = replConnection.getReplicationAPI().replicationStream().logical()
					.withSlotName(replicationSlot).withSlotOption("include-xids", true)
					.withSlotOption("include-timestamp", "on").withSlotOption("skip-empty-xacts", true)
					.withStatusInterval(20, TimeUnit.SECONDS).start();

			Timestamp freshness = new Timestamp(System.currentTimeMillis());
			long cummulativeStaleness = 0L;
			long averageStaleness = 0L;
			long transactionId = 0L;
			long count = 0L;
			long sessionStartTime = System.currentTimeMillis();
			String stalenessFilename = "staleness_" + dateFormat.format(new Date());
			String dataFilename = "data_" + dateFormat.format(new Date());

			Writer stalenessOutPut = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(stalenessFilename, true), "UTF-8"));
			Writer dataWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(dataFilename, true), "UTF-8"));

			sessionEndTime = System.currentTimeMillis() + (120 * 60000);

			while (sessionEndTime > System.currentTimeMillis()) {
				// non blocking receive message

				ByteBuffer msg = stream.readPending();

				if (msg == null) {
					TimeUnit.MILLISECONDS.sleep(10L);
					continue;
				}

				int offset = msg.arrayOffset();
				byte[] source = msg.array();
				int length = source.length - offset;
				// convert byte buffer into string
				String data = new String(source, offset, length);

				// then convert it into bufferedreader
				BufferedReader reader = new BufferedReader(new StringReader(data));
				String line = reader.readLine();

				while (line != null) {
					boolean isTableFound = false;
					// System.out.println(line);

					if (line.contains("BEGIN") || line.contains("COMMIT")) {
						if (line.contains("BEGIN")) {
							transactionId = Long.parseLong(line.split(" ")[1]);
							// System.out.println("TransactionId:
							// "+transactionId);
							ResultSet rs = con.createStatement()
									.executeQuery("select pg_xact_commit_timestamp('" + transactionId + "'::xid)");
							// System.out.println("hello!");
							rs.next();
							Timestamp t = rs.getTimestamp(1);
							if (freshness.before(t)) {
								freshness = t;
								// System.out.println("Freshness: "+ freshness);
							}
							System.out.println(replicationSlot + " "+line);
						} else {
							long staleness = System.currentTimeMillis() - freshness.getTime();
							count++;
							cummulativeStaleness += staleness;
							averageStaleness = cummulativeStaleness / count;

							stalenessOutPut.append((System.currentTimeMillis() - sessionStartTime) + "," + staleness
									+ "," + averageStaleness + "\n");
							// System.out.println("Staleness:"+staleness+"
							// AvgStaleness:"+averageStaleness+ "\n");
							stalenessOutPut.flush();

						}
					} else {
						dataWriter.append(line);
						dataWriter.flush();

					}
					
					line = reader.readLine();
					
				}
				stream.setAppliedLSN(stream.getLastReceiveLSN());
				stream.setFlushedLSN(stream.getLastReceiveLSN());
			}

		} catch (SQLException | InterruptedException | IOException | NumberFormatException e) {
			// TODO Auto-generated catch block
			long errorRecordedTime = (sessionEndTime - System.currentTimeMillis()) / 60000;
			System.out.println("Error recoreded time: " + errorRecordedTime);
			e.printStackTrace();
		}

	}

	public static Connection getConnection() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String user = prop.getProperty("user");
		String password = prop.getProperty("password");
		String url = prop.getProperty("url");

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection(url, connectionProps);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

}
