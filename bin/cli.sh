#!/bin/bash
source /etc/profile
cd `dirname $0`

class=com.dp.blackhole.cli.Cli

cd ..
BLACKHOLE_HOME=`pwd`

libs="$BLACKHOLE_HOME/conf"

for j in `ls $BLACKHOLE_HOME/libs/*.jar`; do
    libs=$libs:$j
done

java -Xmx1024m -cp $libs $class
