package com.kdg.avro;

import org.apache.avro.reflect.Nullable;

public class Customer {

    private String firstName;
    private String lastName;
    @Nullable private String nickName;

    public Customer() {
    }

    public Customer(String firstName, String lastName, String nickName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.nickName = nickName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String fullName(){
        return this.firstName + " " + this.lastName + " " + this.nickName;
    }
}
