sudo apt update

#force use jdk 8 for JNI hardcode and JAVA_HOME
sudo apt install -y -V openjdk-8-jdk

# install needed dependencies
sudo apt install -y -V gcc-10
sudo apt install -y -V g++-10
sudo apt install -y -V cmake
sudo apt install -y -V libomp-dev
sudo apt install -y -V libopenmpi-dev
sudo apt install -y -V libstdc++12
sudo apt install -y -V libssl-dev
sudo apt install -y -V libboost-dev
sudo apt install  -y -V pybind11-dev
sudo apt install  -y -V libgoogle-glog-dev
#thrift build
sudp apt install  -y -V flex 
sudp apt install  -y -V bison
sudo apt install  -y -V libunwind-dev

#https://arrow.apache.org/install/
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-glib-dev # For GLib (C)
sudo apt install -y -V libarrow-dataset-dev # For Apache Arrow Dataset C++
sudo apt install -y -V libarrow-dataset-glib-dev # For Apache Arrow Dataset GLib (C)
sudo apt install -y -V libarrow-flight-dev # For Apache Arrow Flight C++
sudo apt install -y -V libarrow-flight-glib-dev # For Apache Arrow Flight GLib (C)
sudo apt install -y -V libgandiva-dev # For Gandiva C++
sudo apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
sudo apt install -y -V libparquet-dev # For Apache Parquet C++
sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)

pip install "pybind11[global]"
# pin already install librdkafka
#sudo apt install -y -V librdkafka-dev

# cleanup
sudo apt autoremove

#cuda optional
#sudo apt install nvidia-cuda-toolkit



#thrift@0.9 is keg-only, which means it was not symlinked into /opt/homebrew,
#because this is an alternate version of another formula.

#If you need to have thrift@0.9 first in your PATH, run:
#  echo 'export PATH="/opt/homebrew/opt/thrift@0.9/bin:$PATH"' >> /Users/cqin/.bash_profile

#For compilers to find thrift@0.9 you may need to set:
#  export LDFLAGS="-L/opt/homebrew/opt/thrift@0.9/lib"
#  export CPPFLAGS="-I/opt/homebrew/opt/thrift@0.9/include"

#For pkg-config to find thrift@0.9 you may need to set:
#  export PKG_CONFIG_PATH="/opt/homebrew/opt/thrift@0.9/lib/pkgconfig"


#sudo apt install libtorch-dev
#https://linuxhint.com/install-cuda-ubuntu-2004/ follow this tutorial
sudo apt install -y -V build-essential
sudo wget -O /etc/apt/preferences.d/cuda-repository-pin-600 https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/3bf863cc.pub
sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ /
sudo apt update
sudo apt install -y -V cuda-11-8

export CC=/usr/bin/gcc-11
export CXX=/usr/bin/g++-11
export CUDA_ROOT=/usr/local/cuda
sudo ln -s /usr/bin/gcc-11 $CUDA_ROOT/bin/gcc
sudo ln -s /usr/bin/g++-11 $CUDA_ROOT/bin/g++

mkdir build
cd build
wget https://download.pytorch.org/libtorch/cu117/libtorch-cxx11-abi-shared-with-deps-1.13.1%2Bcu117.zip
unzip libtorch-cxx11-abi-shared-with-deps-1.13.1+cu117.zip