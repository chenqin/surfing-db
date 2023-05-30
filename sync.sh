cd ~/matcha/drsquirrel-java/
mvn clean package
cp target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar ../surfing-db-java.jar

cd ~/matcha/drsquirrel-java
mvn clean package
cp target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar ../surfing-db-java.jar
cd ~/matcha/build
make


fh -h drsquirrel-pii-dev > "hostfile.txt"
input="hostfile.txt"

while IFS= read -r line
do
  echo "$line"
  scp ~/matcha/surfing-db-java.jar $line:~/matcha/
  scp ~/matcha/build/MABS $line:~/matcha/build
done < "$input"

rm hostfile.txt