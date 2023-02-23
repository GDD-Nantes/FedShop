# !/bin/sh

cd engines/SPLENDID

# USAGE: SPLENDID.sh <config> <query>
#mainclass=de.uni_koblenz.west.evaluation.QueryProcessingEval
#mainclassfile=src/de/uni_koblenz/west/evaluation/QueryProcessingEval.java

# USAGE: SPLENDID.sh <config>
# EXAMPLE: SPLENDID.sh eval/config.properties
mainclass=de.uni_koblenz.west.evaluation.SourceSelectionEval
mainclassfile=src/de/uni_koblenz/west/evaluation/SourceSelectionEval.java

# USAGE: SPLENDID.sh <config> <query>
#mainclass=de.uni_koblenz.west.splendid.SPLENDID
#mainclassfile=src/de/uni_koblenz/west/splendid/SPLENDID.java

firstserviceclassfile=src/de/uni_koblenz/west/splendid/config/VoidRepositoryFactory.java
secondserviceclassfile=src/de/uni_koblenz/west/splendid/config/FederationSailFactory.java

# set classpath
classpath=./src:./resources

# include all jar files in classpath
for jar in lib/*.jar; do classpath=$classpath:$jar; done

# build SPLENDID
javac -d ./bin -cp $classpath $mainclassfile $firstserviceclassfile $secondserviceclassfile

# run SPLENDID
java -cp $classpath:./bin $mainclass $*

cd ../..