sudo apt update

sudo apt install python3.10 python3-testresources python3-pip libthrift-dev

sudo apt install build-essential openjdk-8-jdk gcc g++ ninja-build libomp-dev libopenmpi-dev libssl-dev libboost-dev pybind11-dev libgoogle-glog-dev flex bison libunwind-dev

# cmake 3.22 works for me
pushd ~
wget https://github.com/Kitware/CMake/releases/download/v3.22.6/cmake-3.22.6-linux-x86_64.tar.gz 
tar vzxf cmake-3.22.6-linux-x86_64.tar.gz 
rm cmake-3.22.6-linux-x86_64.tar.gz
popd

#install cuda-11-8 and depdencies, build pytorch 1.13.1
sudo wget -O /etc/apt/preferences.d/cuda-repository-pin-600 https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/3bf863cc.pub
sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ /"
sudo apt update
sudo apt install cuda-11-8 cuda-toolkit-11-8

sudo ln -s /usr/bin/gcc $CUDA_ROOT/bin/gcc
sudo ln -s /usr/bin/g++ $CUDA_ROOT/bin/g++

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/libcudnn8-dev_8.7.0.84-1+cuda11.8_amd64.deb
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/libcudnn8_8.7.0.84-1+cuda11.8_amd64.deb
sudo dpkg -i libcudnn8*
rm libcudnn8*

#write to env
echo 'export CMAKE_HOME=~/cmake-3.22.6-linux-x86_64' >> ~/.bashrc 
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc 
echo 'export PATH=$PATH:$CMAKE_HOME/bin:$JAVA_HOME/bin' >> ~/.bashrc 
echo 'export CUDA_ROOT=/usr/local/cuda' >> ~/.bashrc 
source ~/.bashrc

#https://arrow.apache.org/install/
sudo apt install ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

sudo apt update
sudo apt install libarrow-dev libarrow-cuda-dev libarrow-glib-dev libarrow-dataset-dev libarrow-dataset-glib-dev libarrow-flight-dev libarrow-flight-glib-dev libgandiva-dev libgandiva-glib-dev libparquet-dev libparquet-glib-dev

sudo pip install "pybind11[global]"
# pin already install librdkafka
#sudo apt install librdkafka-dev


#python download_mnist.py -d build/data/mnist
