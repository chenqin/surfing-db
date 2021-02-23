wget http://www.mpich.org/static/downloads/3.4.1/mpich-3.4.1.tar.gz
tar vzxf mpich-3.4.1.tar.gz
cd mpich-3.4.1
./configure --with-device=ch3
sudo make install all
sudo apt-get install libstdc++6
scp dev-cqin:~/db.tar ~
cd ~
tar -xvf db.tar
