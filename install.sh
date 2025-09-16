mkdir ~/matcha/build
cd ~/matcha/build

sudo apt update

sudo apt install -y python3.10 python3-testresources python3-pip libthrift-dev libsasl2-dev

sudo apt install -y maven

sudo apt install -y build-essential openjdk-8-jdk gcc g++ ninja-build libomp-dev libopenmpi-dev libssl-dev libboost-dev pybind11-dev libgoogle-glog-dev flex bison libunwind-dev

# cmake 3.22 works for me
pushd ~
wget https://github.com/Kitware/CMake/releases/download/v3.22.6/cmake-3.22.6-linux-x86_64.tar.gz 
tar vzxf cmake-3.22.6-linux-x86_64.tar.gz 
rm cmake-3.22.6-linux-x86_64.tar.gz
popd

export CMAKE_HOME=~/cmake-3.22.6-linux-x86_64
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$CMAKE_HOME/bin:$JAVA_HOME/bin


## (CUDA instructions removed)


sudo apt install -y ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

sudo apt update
sudo apt install -y libarrow-dev libarrow-glib-dev libarrow-dataset-dev libarrow-dataset-glib-dev libarrow-flight-dev libarrow-flight-glib-dev libgandiva-dev libgandiva-glib-dev libparquet-dev libparquet-glib-dev

 
#sudo pip install "pybind11[global]"

#git clone https://github.com/edenhill/librdkafka.git 
#cd librdkafka
#./configure --prefix /usr
#make
#sudo make install
