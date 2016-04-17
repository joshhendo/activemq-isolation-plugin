# ActiveMQ Isolation Plugin

** Note that this is a very early work in progress for a proof of concept. What is committed at the moment isn't complete. **

This is a plugin for ActiveMQ that can provide system wide isolation for business objects. It's currently a work in progress and subject to further research.

## Building

This can be built by opening a terminal in the root directory and typing

`mvn clean install`

## Running

This can be run from ActiveMQ with the following commands from the root directory:

`cp target/activemq-isolation.jar ${ACTIVEMQ_HOME}/lib/`
`${ACTIVEMQ_HOME}/bin/activemq xbean:file:./src/main/resources/org/apache/activemq/isolation/activemq-isolation.xml`

