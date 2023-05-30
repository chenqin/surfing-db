source ~/.bashrc
sudo rm /usr/bin/python
sudo ln -s /usr/bin/python3 /usr/bin/python
pip3 install dataclasses typing_extensions
git clone --recursive https://github.com/pytorch/pytorch -b v1.13.1 --depth 1

mkdir -p pytorch/build
mkdir -p libtorch
pushd pytorch/build

# There are a lot of CMake flags, but I'll just go over the important ones. (I'm not completely sure about these flags, note that I've copied most of them from the PKGBUILD from the Arch Linux package repo.)
# BUILD_CUSTOM_PROTOBUF: You can use this to use the Protobuf library installed on your system instead of what is included in the pytorch source.
#            This is sometimes very important; In my case I had a dependency in my project needing a different version of Protobuf, and it collided (quite spectacularily) from the one provided by libtorch.
# BUILD_PYTHON: Use this to enable/disable compiling everything related to Python.
# BUILD_DISTRIBUTED: Set this to enable torch.distributed support.
# USE_SYSTEM_NCCL: Set this to use a system-installed NCCL rather than the one provided by pytorch. 
# NCCL_VERSION: Version of nccl. If pkg-config doesn't work for your OS, then you can just enter this manually.
# CUDA_HOME: This specifies the CUDA directory. Note that this is for Arch Linux; it will be different for other OSs such as Ubuntu/CentOS, and will depend on how you've installed CUDA on your system!
# TORCH_CUDA_ARCH_LIST: This specifies the list of Nvidia architectures you're compiling for. If you use a recent GPU, chances are that you only need to include some of those versions.
# USE_CUDA / USE_CUDNN: Use it to turn on/off CUDA support.
# USE_NATIVE_ARCH: When set to ON, it will compile with all architectural optimizations for your CPU enabled. (For example, AVX, AVX2, AVX512, etc...)
#         Recommend if you're using a recent CPU with special AVX/AVX512 instructions, and if you don't need portable builds.
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
   -DCMAKE_INSTALL_PREFIX=~/libtorch \
   -DGLIBCXX_USE_CXX11_ABI=1 \
   -GNinja \
   ..
   
# We're using ninja to speed up builds. Note that this will take quite some time (it will be good to have a beefy CPU!)
ninja install

popd
