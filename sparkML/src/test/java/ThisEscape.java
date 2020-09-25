import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.awt.*;
import java.util.EventListener;

/**
 * @author: Cock a doodle doo
 * this的逃逸
 */
@ThreadSafe
public class ThisEscape
{
    @GuardedBy("this")
    private int a;
    private int s;

    public ThisEscape(EventSource source){
        source.registorListener(new EventListener(){
            public void onEvent(Event e){
                doSomething();
            }
        });
    }

    public void doSomething(){
        System.out.println("hello");
    }
}
