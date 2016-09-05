# Author: Sushrith Arkal <sushrith.arkal@gmail.com>
PageRank.jar: Node.class Reduce.class Map.class Main.class
	jar cf PageRank.jar *.class

Node.class: Node.java
	javac -cp $$(hadoop classpath)":." Node.java

Reduce.class: Reduce.java
	javac -cp $$(hadoop classpath)":." Reduce.java

Map.class: Map.java
	javac -cp $$(hadoop classpath)":." Map.java

Main.class: Main.java
	javac -cp $$(hadoop classpath)":." -Xlint:deprecation Main.java 
