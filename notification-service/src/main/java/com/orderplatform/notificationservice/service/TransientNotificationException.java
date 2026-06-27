package com.orderplatform.notificationservice.service;

public class TransientNotificationException extends RuntimeException {

    public TransientNotificationException(String message) {
        super(message);
    }
}