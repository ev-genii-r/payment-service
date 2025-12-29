package com.innowise.rudkovskii.service;

public class StatusResolver {

    public static String generateStatus(){
        if(RandomIntegerGenerator.getRandomInteger() % 2 == 0){
            return "SUCCESS";
        }else{
            return "FAILED";
        }
    }

}
