import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Client {

	static Session session;
	static ResultSet resultSet;
	static Row row;

	public static void main( String[] args ) {
		Cluster cluster = Cluster.builder()
				.addContactPoints("127.0.0.1")
				.build();

		session = cluster.connect();
		query1();
		query2();
		query3();
		query4();
		query5();
		alternateQuery1();
		System.exit(0);
	}

	public static void query1() {
		System.out.println("Executing query 1...");
		String cqlStatement1 = "SELECT count(*) FROM cs510.freeway_data where dqflags = 4;";
		resultSet = session.execute(cqlStatement1);
		row = resultSet.one();
		System.out.println("\tNumber of speeds > 100 = " + row.getLong("count") + "\n");
	}

	public static void alternateQuery1() {
		System.out.println("\nExecuting alternate query 1...");
		long count, sum = 0;
		for (String valueOfDQFlags : Constants.arrayOfPossibleDqflagsWithBit3SetTo1)  {
			String cqlStatement1 = "SELECT count(*) FROM cs510.freeway_data where dqflags = " + valueOfDQFlags + ";";
			resultSet = session.execute(cqlStatement1);
			Row row = resultSet.one();
			count = row.getLong("count");
			sum += count;
			System.out.println("\t\tdqflags value: " + valueOfDQFlags + ",\t Records returned: " + count);
		}
		System.out.println("\tNumber of speeds > 100 = " + sum + "\n");
	}

	public static void query2() {
		System.out.println("Executing query 2...");

		String cqlStatement1 = "SELECT sum(volume) as sum FROM cs510.freeway_data WHERE locationtext = 'Foster NB' AND starttime >= '2011-09-21 00:00:00+0000' AND starttime < '2011-09-22 00:00:00+0000' ALLOW FILTERING;";
		resultSet = session.execute(cqlStatement1);
		row = resultSet.one();
		int volume = row.getInt("sum");
		System.out.println("\tTotal volume for the station Foster NB on 21st Sept 2011 is: " + volume + "\n");
	}

	public static void query3() {
		System.out.println("Executing query 3...");

		String cqlStatement3 = "SELECT length FROM cs510.station_data WHERE locationtext = 'Foster NB'";
		resultSet = session.execute(cqlStatement3);
		row = resultSet.one();
		float length = row.getFloat("length");
		System.out.println("\tLength of station 'Foster NB' is: " + length);

		String cqlStatement1 = "SELECT sum(speed) as sum FROM cs510.freeway_data WHERE locationtext = 'Foster NB' AND starttime >= '2011-09-22 07:00:00+0000' AND starttime <= '2011-09-22 09:00:00+0000' ALLOW FILTERING;";
		resultSet = session.execute(cqlStatement1);
		row = resultSet.one();
		int sumSpeed7To9AM = row.getInt("sum");
		cqlStatement1 = "SELECT count(speed) as count FROM cs510.freeway_data WHERE locationtext = 'Foster NB' AND starttime >= '2011-09-22 07:00:00+0000' AND starttime <= '2011-09-22 09:00:00+0000' ALLOW FILTERING;";
		resultSet = session.execute(cqlStatement1);
		row = resultSet.one();
		long countSpeed7To9AM = row.getLong("count");
		float avgSpeed7To9AM = ((float) sumSpeed7To9AM)/countSpeed7To9AM;
		System.out.println("\n\tSum: " + sumSpeed7To9AM + ", Count: " + countSpeed7To9AM);
		System.out.println("\tAvg speed (7-9am): " + avgSpeed7To9AM);
		System.out.println("\tAvg travel time for 7-9 am is: " + (length / avgSpeed7To9AM) * 3600 + " seconds");

		String cqlStatement2 = "SELECT sum(speed) as sum FROM cs510.freeway_data WHERE locationtext = 'Foster NB' AND starttime >= '2011-09-22 16:00:00+0000' AND starttime <= '2011-09-22 18:00:00+0000' ALLOW FILTERING;";
		resultSet = session.execute(cqlStatement2);
		row = resultSet.one();
		int sumSpeed4To6PM = row.getInt("sum");
		cqlStatement2 = "SELECT count(speed) as count FROM cs510.freeway_data WHERE locationtext = 'Foster NB' AND starttime >= '2011-09-22 16:00:00+0000' AND starttime <= '2011-09-22 18:00:00+0000' ALLOW FILTERING;";
		resultSet = session.execute(cqlStatement2);
		row = resultSet.one();
		long countSpeed4To6PM = row.getLong("count");
		float avgSpeed4To6PM = (float) sumSpeed4To6PM/countSpeed4To6PM;
		System.out.println("\n\tSum: " + sumSpeed4To6PM + ", Count: " + countSpeed4To6PM);
		System.out.println("\tAvg speed (4-6pm): " +avgSpeed4To6PM);
		System.out.println("\tAvg travel time for 4-6 pm is: " + (length / avgSpeed4To6PM) * 3600 + " seconds\n");
	}
	public static void query4() {
		System.out.println("Executing query 4...");

		int i = 0;
		int endStationId = 0;
		List<LocationText> usPath = new ArrayList<LocationText>();
		LocationText lt;
		String destinationLocationText = "";

		String cqlStatement1 = "SELECT stationid, locationtext, upstream, downstream FROM  cs510.station_data WHERE shortdirection = 'N';";
		ResultSet s = session.execute(cqlStatement1);
		while (s.iterator().hasNext()) {
			Row r = s.one();
			if (r.getString(Constants.locationText).toLowerCase().startsWith(Constants.johnsonCrk)) {
				lt = new LocationText();
				lt.usStationId = r.getInt(Constants.upstream);
				lt.dsStationId = r.getInt(Constants.downstream);
				lt.uslocationText =  r.getString(Constants.locationText);
				lt.dslocationText =  r.getString(Constants.locationText);

				usPath.add(lt);
			}
			else if(r.getString(Constants.locationText).toLowerCase().startsWith(Constants.columbiaCrk)) {
				endStationId = r.getInt(Constants.stationid);
				destinationLocationText = r.getString(Constants.locationText);
			}
		}
		do {
			i++;
			lt = new LocationText();
			cqlStatement1 = "SELECT downstream, locationtext FROM  cs510.station_data WHERE stationid = ?;";
			PreparedStatement ps = session.prepare(cqlStatement1);
			s = session.execute(ps.bind(usPath.get(i-1).dsStationId));
			while (s.iterator().hasNext()) {
				Row r = s.one();
				lt.dsStationId = r.getInt(Constants.downstream);
				lt.dslocationText = r.getString(Constants.locationText);
			}
			cqlStatement1 = "SELECT upstream, locationtext FROM  cs510.station_data WHERE stationid = ?;";
			ps = session.prepare(cqlStatement1);
			s = session.execute(ps.bind(usPath.get(i-1).usStationId));
			while (s.iterator().hasNext()) {
				Row r = s.one();
				lt.usStationId = r.getInt(Constants.upstream);
				lt.uslocationText = r.getString(Constants.locationText);
			}
			usPath.add(lt);
		}while(!(usPath.get(i).usStationId == endStationId || usPath.get(i).dsStationId == endStationId));

		if (usPath.get(i).usStationId == endStationId) {
			System.out.println("\tThe path from Johnson Creek to Columbia Blvd is:");
			for(i = 0; i < usPath.size(); i++)
				System.out.print(usPath.get(i).uslocationText +"--->");
		}
		else if (usPath.get(i).dsStationId == endStationId) {
			System.out.println("\tThe path from Johnson Creek to Columbia Blvd is");
			for(i = 0; i < usPath.size(); i++)
				System.out.print(usPath.get(i).dslocationText +"--->");
		}
		System.out.println(destinationLocationText);
	}

	public static void query5() {
		System.out.println("\nExecuting query 5...");
		String cqlStatement = "SELECT length FROM cs510.station_data WHERE stationid = 1140";
		resultSet = session.execute(cqlStatement);
		row = resultSet.one();
		System.out.println("\tOriginal length of station with stationid = 1140 is: " + row.getFloat("length"));
		System.out.println("\tUpdating length of the station to 2.3");
		String cqlStatement1 = "UPDATE cs510.station_data SET length = 2.3 WHERE stationid = 1140";
		session.execute(cqlStatement1);

		resultSet = session.execute(cqlStatement);
		row = resultSet.one();
		System.out.println("\tUpdated length of station with stationid = 1140 is: " + row.getFloat("length"));

		//Updating length of station 1140 back to original value of 2.14 so query results are the same for each run.
		cqlStatement1 = "UPDATE cs510.station_data SET length = 2.14 WHERE stationid = 1140";
		session.execute(cqlStatement1);
	}
}