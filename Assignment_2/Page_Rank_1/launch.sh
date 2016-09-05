cd /home/anush/Desktop/hmm
sudo javac -classpath $(hadoop classpath) matrix.java
jar cf /home/hduser/matrix.jar matrix*.class
sudo cp /home/hduser/matrix.jar .
hadoop jar matrix.jar matrix /user/input /user/input1 /user/output
echo "********************************OUTPUT*******************************"
hadoop fs -cat /user/output/*
echo "*********************************************************************"

