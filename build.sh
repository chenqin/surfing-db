export CMAKE_HOME=~/cmake-3.22.6-linux-x86_64
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$CMAKE_HOME/bin:$JAVA_HOME/bin
mkdir ~/matcha/build
cd ~/matcha/build
cmake ..
make
