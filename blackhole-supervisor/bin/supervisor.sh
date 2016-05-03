#!/bin/bash
source /etc/profile
cd `dirname $0`

class=com.dp.blackhole.supervisor.Supervisor

cd ..
BLACKHOLE_HOME=`pwd`

lastword=${class##*.}
name=${lastword,,}
out=$BLACKHOLE_HOME/logs/$name.out
pid=$BLACKHOLE_HOME/$name.pid
psfile=$BLACKHOLE_HOME/blackhole.ps

libs="$BLACKHOLE_HOME/conf"

if [ "x$1" == "xstop" ]; then
    echo "stop $name"
    kill -9 `cat $pid` > /dev/null 2>&1
    exit 0
fi

ps aux > $psfile

if [ `grep -c $class $psfile` -ne 0 ]; then
    echo "$name is running, stop it first"
    exit 1
fi

for j in `ls $BLACKHOLE_HOME/libs/*.jar`; do
    libs=$libs:$j
done

echo "starting $name"
nohup java -Xmx1024m -cp $libs $class > $out 2>&1 &
echo $! > $pid
