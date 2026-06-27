package com.orderplatform.emailservice.service;

public class TransientEmailException extends RuntimeException {

    public TransientEmailException(String message) {
        super(message);
    }
}