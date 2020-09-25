/**
 * @author: Cock a doodle doo
 */
public class Widget
{
    public synchronized void doSomething()
    {
        System.out.println("Widget中的this: " + this);
    }
}
