package org.apache.activemq.isolation;

public class NoLockException extends Exception {
	public NoLockException() {}

	public NoLockException(String message) {
		super(message);
	}
}
