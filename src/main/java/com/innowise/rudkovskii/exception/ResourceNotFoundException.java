package com.innowise.rudkovskii.exception;

public class ResourceNotFoundException extends RuntimeException{

    public ResourceNotFoundException(String resource) {
        super(resource + " not found");
    }

}
