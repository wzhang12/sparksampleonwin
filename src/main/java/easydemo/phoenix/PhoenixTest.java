package easydemo.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PhoenixTest {
	private Connection conn;
	private Statement stmt;
	private ResultSet rs;
	
	@Before
	public void initResource() throws Exception {
		Class.forName(ConfigUtil.getDriver());
		conn = DriverManager.getConnection(ConfigUtil.getUrl(), ConfigUtil.getUsername(), ConfigUtil.getPassword());
		stmt = conn.createStatement();
	}
	
	@Test
	public void testCreateTab() throws Exception {
		String sql = "create table test7(mykey integer not  null primary key,mycolumn varchar)";
		stmt.execute(sql);
		conn.commit();
	}
	
	@Test
	public void testInsert() throws Exception {
		String sql1 = "upsert into test7 values(4,'test1')";
		String sql2 = "upsert into test7 values(5,'test2')";
		String sql3 = "upsert into test7 values(6,'test3')";
		stmt.executeUpdate(sql1);
		stmt.executeUpdate(sql2);
		stmt.executeUpdate(sql3);
		conn.commit();
	}
	
	@Test
	public void testUpdate() throws Exception {
		String sql1 = "upsert into test2 values(1,'test0000')";
		stmt.executeUpdate(sql1);
		conn.commit();
	}
	
	@Test
	public void testDelete() throws Exception {
		String sql1 = "delete from test6 where mykey=4";
		String sql2 = "delete from test6 where mykey=5";
		String sql3 = "delete from test6 where mykey=6";
		stmt.executeUpdate(sql1);
		stmt.executeUpdate(sql2);
		stmt.executeUpdate(sql3);
		conn.commit();
	}
	
	@After
	public void closeResource()  throws Exception {
		if(rs != null) {
			rs.close();
		}
		if(stmt != null) {
			stmt.close();
		}
		if(conn != null) {
			conn.close();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
