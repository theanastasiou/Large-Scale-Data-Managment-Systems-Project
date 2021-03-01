Arguments και Εντολές εκτέλεσης :

vagrant up
vagrant ssh
hadoop/sbin/start-dfs.sh
spark/sbin/start-all.sh

Για να τρέξει το πρόγραμμα χρείαζεται να υπάρχει κάπου το αρχείο en-pos-maxent.bin για την opennlp.
 
Arguments: 0 - > input folder
        1 - > output folder
        2 - > size of Window
        3 -> number of iterations
        4 - > how many words for ranking
        5 -> en-pos-maxent.bin path arxeiou tagger    

Εντολή για υλοποίηση μέσω Vagrant :
spark/bin/spark-submit --class com.mycompany.mavenproject1.StopWords /vagrant/data/ergasia_spark/mavenproject1/target/mavenproject1-1.0-SNAPSHOT-shaded.jar hdfs://localhost:54310/ergasia_spark/input hdfs://localhost:54310/ergasia_spark/output 2 4 5 /vagrant/data/ergasia_spark/en-pos-maxent.bin

Εντολη υλοποίησης μέσω IntelliJ/Netbeans terminal :
mvn exec:java -Dexec.mainClass=com.mycompany.ergbigdta.StopWords -Dexec.args=" /home/angelo/Desktop/BigData/Inspec/docsutf8 /home/angelo/Desktop/BigData/Kwstas/NetBeansProjects/output 2 3 5 /home/angelo/Desktop/BigData/Kwstas/NetBeansProjects/en-pos-maxent.bin"

