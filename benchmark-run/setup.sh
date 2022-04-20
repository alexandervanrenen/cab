
# Create and mount ssd
sudo mkfs -t ext4 /dev/nvme1n1
sudo mkdir /media/ssd
sudo mount /dev/nvme1n1 /media/ssd
sudo chown -R ec2-user:ec2-user /media/ssd/

# Install dependencies
sudo yum update -y
sudo yum install -y gcc zstd make git tmux

# Install nodejs
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.38.0/install.sh | bash
source ~/.bashrc
nvm install v17.7.2

# Install our dependencies (run in benchmark dir)
cd benchmark
npm install

# Install dbgen
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make
cd ..