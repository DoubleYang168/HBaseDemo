package cn.edu.hhu.dingy.entity;

public class HanJang {
    private String value;
    private String longitude;
    private String latitude;
    private String date;

    public HanJang() {
    }

    public HanJang(String value, String longitude, String latitude, String date) {
        this.value = value;
        this.longitude = longitude;
        this.latitude = latitude;
        this.date = date;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "HanJang{" +
                "value='" + value + '\'' +
                ", longitude='" + longitude + '\'' +
                ", latitude='" + latitude + '\'' +
                ", date='" + date + '\'' +
                '}';
    }
}
