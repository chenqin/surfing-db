sudo apt update

sudo apt install -y -V build-essential

#force use jdk 8 for JNI hardcode and JAVA_HOME
sudo apt install -y -V openjdk-8-jdk

# install needed dependencies
sudo apt install -y -V gcc
sudo apt install -y -V g++
sudo apt install -y -V ninja-build
sudo apt install -y -V libomp-dev
sudo apt install -y -V libopenmpi-dev
sudo apt install -y -V libstdc++12
sudo apt install -y -V libssl-dev
sudo apt install -y -V libboost-dev
sudo apt install  -y -V pybind11-dev
sudo apt install  -y -V libgoogle-glog-dev
sudp apt install  -y -V flex 
sudp apt install  -y -V bison
sudo apt install  -y -V libunwind-dev

#https://arrow.apache.org/install/
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-cuda-dev #For GPU
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

#install cuda-11-8 and depdencies, build pytorch 1.13.1
sudo wget -O /etc/apt/preferences.d/cuda-repository-pin-600 https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/3bf863cc.pub
sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ /"
sudo apt update
sudo apt install -y -V cuda-11-8
sudo apt install -y -V cuda-toolkit-11-8

export CUDA_ROOT=/usr/local/cuda
sudo ln -s /usr/bin/gcc $CUDA_ROOT/bin/gcc
sudo ln -s /usr/bin/g++ $CUDA_ROOT/bin/g++

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/libcudnn8-dev_8.7.0.84-1+cuda11.8_amd64.deb
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/libcudnn8_8.7.0.84-1+cuda11.8_amd64.deb
sudo dpkg -i libcudnn8*
rm libcudnn8*

git clone --recursive https://github.com/pytorch/pytorch -b v1.13.1 --depth 1

mkdir -p pytorch/build
mkdir -p libtorch
pushd pytorch/build

# There are a lot of CMake flags, but I'll just go over the important ones. (I'm not completely sure about these flags, note that I've copied most of them from the PKGBUILD from the Arch Linux package repo.)
# BUILD_CUSTOM_PROTOBUF: You can use this to use the Protobuf library installed on your system instead of what is included in the pytorch source.
#                        This is sometimes very important; In my case I had a dependency in my project needing a different version of Protobuf, and it collided (quite spectacularily) from the one provided by libtorch.
# BUILD_PYTHON: Use this to enable/disable compiling everything related to Python.
# BUILD_DISTRIBUTED: Set this to enable torch.distributed support.
# USE_SYSTEM_NCCL: Set this to use a system-installed NCCL rather than the one provided by pytorch. 
# NCCL_VERSION: Version of nccl. If pkg-config doesn't work for your OS, then you can just enter this manually.
# CUDA_HOME: This specifies the CUDA directory. Note that this is for Arch Linux; it will be different for other OSs such as Ubuntu/CentOS, and will depend on how you've installed CUDA on your system!
# TORCH_CUDA_ARCH_LIST: This specifies the list of Nvidia architectures you're compiling for. If you use a recent GPU, chances are that you only need to include some of those versions.
# USE_CUDA / USE_CUDNN: Use it to turn on/off CUDA support.
# USE_NATIVE_ARCH: When set to ON, it will compile with all architectural optimizations for your CPU enabled. (For example, AVX, AVX2, AVX512, etc...)
#                  Recommend if you're using a recent CPU with special AVX/AVX512 instructions, and if you don't need portable builds.
# Note that there might be some unnecessary flags turned on. Tweak this depending on your system and requirements.

cmake -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc \
      -DUSE_MKLDNN=ON \
      -DBUILD_CUSTOM_PROTOBUF=OFF \
      -DBUILD_SHARED_LIBS=ON \
      -DUSE_FFMPEG=ON \
      -DUSE_GFLAGS=ON \
      -DUSE_GLOG=ON \
      -DBUILD_BINARY=OFF \
      -DBUILD_PYTHON=OFF \
      -DBUILD_TEST=OFF \
      -DUSE_OPENCV=ON \
      -DUSE_SYSTEM_NCCL=ON \
      -DBUILD_CAFFE2=ON \
      -DUSE_DISTRIBUTED=ON \
      -DUSE_MPI=ON \
      -DUSE_C10D_MPI=ON \
      -DCUDAHOSTCXX=g++ \
      -DCUDA_HOME=/usr/local/cuda \
      -DCUDNN_LIB_DIR=/usr/lib/x86_64-linux-gnu \
      -DCUDNN_INCLUDE_DIR=/usr/include \
      -DTORCH_CUDA_ARCH_LIST="6.0 6.1 7.0+PTX 8.0" \
      -DUSE_CUDA=ON \
      -DUSE_FAST_NVCC=ON \
      -DUSE_CUDNN=ON \
      -DCAFFE2_STATIC_LINK_CUDA=ON \
      -DUSE_STATIC_CUDNN=ON \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=../../libtorch \
      -DGLIBCXX_USE_CXX11_ABI=1 \
      -GNinja \
      ..
      
# We're using ninja to speed up builds. Note that this will take quite some time (it will be good to have a beefy CPU!)
ninja install

popd

# cmake 3.22 works for me
wget https://github.com/Kitware/CMake/releases/download/v3.22.6/cmake-3.22.6-linux-x86_64.tar.gz ~
tar vzxf ~/cmake-3.22.6-linux-x86_64.tar.gz 

#write to env
echo 'export CMAKE_HOME=~/cmake-3.22.6-linux-x86_64' >> ~/.bashrc 
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc 
echo 'export PATH=$PATH:$CMAKE_HOME/bin:$JAVA_HOME/bin' >> ~/.bashrc 
echo 'export CUDA_ROOT=/usr/local/cuda' >> ~/.bashrc 
source ~/.bashrc

python download_mnist.py -d build/data/mnist