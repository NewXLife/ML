/**
 * @author: Cock a doodle doo
 */
public class LoggingWidget extends Widget
{
    @Override
    public synchronized void doSomething() {
        super.doSomething();
        System.out.println("LoggingWidget中的super: " + super.toString());
        System.out.println("LoggingWidget中的this: " + this);
    }

    public static void main(String[] args) {
        LoggingWidget loggingWidget = new LoggingWidget();
        System.out.println(loggingWidget);
        loggingWidget.doSomething();
    }

}
