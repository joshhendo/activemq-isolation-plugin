package org.apache.activemq.isolation.exceptions;

public class NoLockException extends Exception {
	public NoLockException() {}

	public NoLockException(String message) {
		super(message);
	}
}
