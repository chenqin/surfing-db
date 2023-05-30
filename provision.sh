fh -h drsquirrel-pii-dev > "hostfile.txt"
input="hostfile.txt"

while IFS= read -r line
do
  echo "$line"
  ssh-copy-id cqin@$line
  ssh -A cqin@$line -t 'git clone git@github.com:cqin_pins/matcha.git'
  ssh -A cqin@$line  '/home/cqin/matcha/install.sh'
  ssh -A cqin@$line  '/home/cqin/matcha/build.sh'
done < "$input"

rm hostfile.txt

#ssh-copy-id cqin@$PROCESS
#ssh -A cqin@$PROCESS -t 'git clone git@github.com:cqin_pins/matcha.git'
#ssh -A cqin@$PROCESS  '/home/cqin/matcha/install.sh'
#ssh -A cqin@$PROCESS  '/home/cqin/matcha/build.sh'
