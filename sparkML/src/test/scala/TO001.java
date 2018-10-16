public class TO001 {
    public static void main(String[] args){
        String ds = null;
        System.out.println(getId(ds));
    }

    public static  long getId(String ds) {
        if (ds == null){
            return -100;
        }
        return 1;
    }
}
