package club.cleland.hadoop_basics.io;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserWritable implements WritableComparable<UserWritable> {
    private String id = "";
    private String name = "";
    private int score = 0;
    private int age = 0;

    // 在反序列化时，反射机制需要调用空参构造函数
    public UserWritable(){};

    public UserWritable(String id, String name, int score, int age){
        this.id = id;
        this.name = name;
        this.score = score;
        this.age = age;
    }

    public String getName(){
        return this.name;
    }

    public void setName(String name){
        this.name = name;
    }

    public String getId(){
        return this.id;
    }

    public void setId(String id){
        this.id = id;
    }

    public int getScore(){
        return this.score;
    }

    public void setScore(int score){
        this.score = score;
    }

    public int getAge(){
        return this.age;
    }

    public void setAge(int age){
        this.age = age;
    }

    public void set(String id, String name, int score, int age){
        this.id = id;
        this.name = name;
        this.score = score;
        this.age = age;
    }

    @Override
    public int compareTo(UserWritable o){
        int result  = this.name.compareTo(o.getName());
        if (result != 0){
            return result;
        }

        return this.score > o.getScore() ? 1 : -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeUTF(this.id);
        dataOutput.writeUTF(this.name);
        dataOutput.writeInt(this.score);
        dataOutput.writeShort(this.age);
    }

    @Override
    public void readFields(DataInput dataInput) throws  IOException{
        this.id = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.score = dataInput.readInt();
        this.age = dataInput.readShort();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserWritable that = (UserWritable) o;

        if (score != that.score) return false;
        if (age != that.age) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode(){
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + score;
        result = 31 * result + (int) age;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString(){
        return "name='" + name + '\'' +
                ", score=" + score +
                ", age=" + age +
                ", id='" + id + "'";
    }
}

