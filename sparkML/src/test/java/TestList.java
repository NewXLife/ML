import java.util.ArrayList;
import java.util.List;

public class TestList {
    public static void main(String args[]){
        List<String> list1 = new ArrayList<>();
        list1.add("1");
        list1.add("1");
        list1.add("1");

        List<String> list2 = new ArrayList<>();
        list2.add("2");
        list2.add("2");
        list2.add("2");
        list2.add("2");

        list1.addAll(list2);
        for(String a: list1){
            System.out.println(a);
        }
    }
}
