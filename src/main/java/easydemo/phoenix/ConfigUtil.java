package easydemo.phoenix;

import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {
	private static Properties p = new Properties();
	
	static {
		try {
			p.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getDriver() {
		return p.getProperty("phoenix.driver");
	}
	
	public static String getUrl() {
		return p.getProperty("phoenix.url");
	}
	
	public static String getUsername() {
		return p.getProperty("phoenix.username");
	}
	
	public static String getPassword() {
		return p.getProperty("phoenix.password");
	}
	
	
	
	public static void main(String []args) {
		System.out.println(getDriver());
		System.out.println(getUrl());
		System.out.println(getPassword());
		System.out.println(getUsername());
	}
	
	
	
	
	
	
	
}
