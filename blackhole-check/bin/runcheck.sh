cd `dirname $0`
export LANG="en_US.UTF-8"
class=com.dp.blackhole.check.CheckDone
cd ..
CHECK_HOME=`pwd`
libs="$CHECK_HOME/conf"
jars=`ls $CHECK_HOME/libs/*jar`

for j in $jars; do
    libs=$libs:$j
done

echo "checkdone start"
nohup java -Xmx2048m -XX:PermSize=64m -XX:MaxPermSize=256m -cp $libs $class > $CHECK_HOME/check.out 2>&1 &
