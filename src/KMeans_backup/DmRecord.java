package KMeans_backup;

public class DmRecord {
    private String name;
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private double xpodouble;
    private double ypodouble;
    private double distance;     //与类中心距离的平方
   
    public DmRecord(){
       
    }
   
    public DmRecord(String name,double x,double y,double dis){
        this.name = name;
        this.xpodouble = x;
        this.ypodouble = y;
        this.distance=dis;
    }

    public double getXpoint() {
        return xpodouble;
    }

    public void setXpoint(double xpodouble) {
        this.xpodouble = xpodouble;
    }

    public double getYpoint() {
        return ypodouble;
    }

    public void setYpoint(double ypodouble) {
        this.ypodouble = ypodouble;
    }
   
    public double getDis(){
    	return distance;
    }
    
    public  double distance(DmRecord record){
        return Math.sqrt(Math.pow(this.xpodouble-record.xpodouble, 2)+Math.pow(this.ypodouble-record.ypodouble, 2));
    }
}
