cd /home/anush/Desktop/Page_Rank_1
sudo javac -classpath $(hadoop classpath) matrix_map.java
sudo javac -classpath $(hadoop classpath) matrix_reduce.java
sudo javac -classpath $(hadoop classpath) main.java
sudo chmod 777 *
jar cf /home/hduser/PageRank.jar main*.class matrix_map*.class matrix_reduce*.java
sudo cp /home/hduser/PageRank.jar .
hadoop jar PageRank.jar main /user/input /user/output
echo "********************************OUTPUT*******************************"
hadoop fs -cat /user/output/*
echo "*********************************************************************"

