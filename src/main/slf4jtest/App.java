package slf4jtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	  final Logger logger = LoggerFactory.getLogger(App.class);

	    private void test() {
	        logger.info("这是一条日志信息 - {}", "mafly");
	        
	    }

	    public static void main(String[] args) {
	        App app = new App();
	        app.test();
	        
	        System.out.println("Hello World!");
	        
	        try {
				Thread.sleep(1000*60);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        app.logger.info("test");
	    }
}
