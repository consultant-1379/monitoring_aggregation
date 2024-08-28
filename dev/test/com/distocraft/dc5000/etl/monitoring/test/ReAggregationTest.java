/**
 * 
 */
package com.distocraft.dc5000.etl.monitoring.test;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ssc.rockfactory.RockFactory;
import com.distocraft.dc5000.etl.monitoring.ReAggregation;

/**
 * @author eninkar
 *
 */
public class ReAggregationTest {
	
	
	private ReAggregation objUnderTest;
	

	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		final String ENGINE_DB_URL = "jdbc:hsqldb:mem:testdb";
		final String ENGINE_DB_USERNAME = "SA";
		final String ENGINE_DB_PASSWORD = "";
		final String ENGINE_DB_DRIVERNAME = "org.hsqldb.jdbcDriver";
	//	System.setProperty("dc5000.config.directory", "/eniq/sw/conf");
	//    StaticProperties.reload();
		RockFactory rock= new RockFactory(ENGINE_DB_URL, ENGINE_DB_USERNAME, ENGINE_DB_PASSWORD, ENGINE_DB_DRIVERNAME, "", true);
		Statement stmt = rock.getConnection().createStatement();
		try{
			stmt.executeUpdate("drop table Meta_databases");
		}
		catch(Exception e) {}
		stmt.executeUpdate("create table Meta_databases (USERNAME VARCHAR(30), VERSION_NUMBER VARCHAR(32), TYPE_NAME VARCHAR(15), CONNECTION_ID NUMERIC(38), CONNECTION_NAME VARCHAR(30), CONNECTION_STRING VARCHAR(200), PASSWORD VARCHAR(30), DESCRIPTION VARCHAR(32000), DRIVER_NAME VARCHAR(100), DB_LINK_NAME VARCHAR(128))");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', '0', 'etlrep', 'jdbc:hsqldb:mem:testdbr', '', 'ETL Repository Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', '1', 'dwhrep', 'jdbc:hsqldb:mem:testdbr', '', 'DWH Repository Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA',  '0', 'USER', '2', 'dc', 'jdbc:hsqldb:mem:testdbr', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '3', 'DBA', 'jdbc:hsqldb:mem:testdbr', '', 'ETL Repository Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '4', 'DBA', 'jdbc:hsqldb:mem:testdbr', '', 'DWH Repository Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'DBA', '5', 'DBA', 'jdbc:hsqldb:mem:testdbr', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases (USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('dba', '0', 'DBA', '7', 'dwh', 'jdbc:hsqldb:mem:testdbr', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver')");
		stmt.executeUpdate("insert into Meta_databases( USERNAME, VERSION_NUMBER, TYPE_NAME, CONNECTION_ID, CONNECTION_NAME, CONNECTION_STRING, PASSWORD, DESCRIPTION, DRIVER_NAME) values ('SA', '0', 'USER', 2, 'dwh', 'jdbc:hsqldb:mem:dwh', '', 'The DataWareHouse Database', 'org.hsqldb.jdbcDriver')");
		objUnderTest = new ReAggregation(Logger.getAnonymousLogger(), rock);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testLastWeek(){
		
		Method m = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
		Date day = new Date();
		String lastWeek = null;
		
		try {
			m = ReAggregation.class.getDeclaredMethod("lastWeek", String.class);
			m.setAccessible(true);
			String date = sdf.format(day);
			
			 lastWeek = (String) m.invoke(objUnderTest, date );
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Assert.assertEquals("2010-08-13 00:00:00", lastWeek);
		Assert.assertNotNull(lastWeek);
	}

}
