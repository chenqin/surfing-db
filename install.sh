sudo apt update

# install needed dependencies
sudo apt install -y -V g++ 
sudo apt install -y -V cmake
sudo apt install -y -V libomp-dev
sudo apt install -y -V libopenmpi-dev
sudo apt install -y -V libstdc++6
sudo apt install -y -V libssl-dev
sudo apt install -y -V libboost-dev
sudo apt install  -y -V pybind11-dev
sudo apt install  -y -V libgoogle-glog-dev
sudp apt install  -y -V flex bison
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
# Notes for Plasma related packages:
#   * You need to enable "non-free" component on Debian GNU/Linux
#   * You need to enable "multiverse" component on Ubuntu
#   * You can use Plasma related packages only on amd64
#sudo apt install -y -V libplasma-dev # For Plasma C++
#sudo apt install -y -V libplasma-glib-dev # For Plasma GLib (C)
sudo apt install -y -V libgandiva-dev # For Gandiva C++
sudo apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
sudo apt install -y -V libparquet-dev # For Apache Parquet C++
sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)

pip install "pybind11[global]"
# pin already install librdkafka
#sudo apt install -y -V librdkafka-dev

# cleanup
sudo apt autoremove



#thrift@0.9 is keg-only, which means it was not symlinked into /opt/homebrew,
#because this is an alternate version of another formula.

#If you need to have thrift@0.9 first in your PATH, run:
#  echo 'export PATH="/opt/homebrew/opt/thrift@0.9/bin:$PATH"' >> /Users/cqin/.bash_profile

#For compilers to find thrift@0.9 you may need to set:
#  export LDFLAGS="-L/opt/homebrew/opt/thrift@0.9/lib"
#  export CPPFLAGS="-I/opt/homebrew/opt/thrift@0.9/include"

#For pkg-config to find thrift@0.9 you may need to set:
#  export PKG_CONFIG_PATH="/opt/homebrew/opt/thrift@0.9/lib/pkgconfig"