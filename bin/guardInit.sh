#!/bin/bash
cd ..
TEST_HOME=`pwd`
libs=$TEST_HOME
libs=$libs:"$TEST_HOME"/libs/commons-logging-1.1.1.jar:"$TEST_HOME"/libs/blackhole-agent-guarder-2.0.0-SNAPSHOT.jar
cd bin
#java -cp $libs com.dp.blackhole.agent.guarder.GuardInit guard
java -cp $libs com.dp.blackhole.agent.guarder.GuardInit shutdown
#java -cp $libs com.dp.blackhole.agent.guarder.GuardInit restart
